/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package pipeline orchestrates the snapshot download: tree build, resume scan,
// bounded-concurrency node processing, volume download, and finalization.
package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/semaphore"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	defaultWorkers              = 4
	defaultPerVolumeConcurrency = 4
	defaultMaxParallelDownloads = 5
	defaultTTL                  = "2h"
	defaultReadinessTimeout     = 5 * time.Minute
	defaultReleaseTimeout       = 30 * time.Second
)

// Config holds all parameters for a snapshot download run.
type Config struct {
	// Namespace is the Kubernetes namespace of the root Snapshot.
	Namespace string

	// RootSnapshot is the metadata.name of the root Snapshot CR.
	RootSnapshot string

	// OutputDir is the absolute path of the root output directory.
	// Its basename is chosen by the caller; no namespace wrapper is added.
	OutputDir string

	// Workers is the maximum number of nodes processed concurrently (default: 4).
	// For block volumes each worker holds up to PerVolumeConcurrency chunks in
	// memory simultaneously; worst-case RSS ≈ Workers × PerVolumeConcurrency ×
	// (ChunkSize + compressed frame overhead).  With defaults 4×4×256 MiB this
	// is roughly 4–5 GiB.  Reduce Workers or ChunkSize on memory-constrained hosts.
	Workers int

	// PerVolumeConcurrency is the maximum number of parallel chunk or file
	// downloads per volume (default: 4).
	// Multiplied with Workers and ChunkSize it determines the worst-case RSS;
	// see Workers for the peak formula.
	PerVolumeConcurrency int

	// MaxParallelDownloads is the global cap on concurrent whole-volume-stream
	// downloads across all nodes (default: 5). It is enforced by a single shared
	// semaphore acquired once per volume stream in downloadVolumeBinding.
	// This cap is independent of Workers (node-level errgroup limit) and
	// PerVolumeConcurrency (chunk/file-level errgroup limit per volume).
	MaxParallelDownloads int

	// streamSem is the shared semaphore enforcing MaxParallelDownloads. It is
	// created once in applyDefaults (sized to MaxParallelDownloads) and shared as
	// a pointer across value-copied per-node configs, so all node goroutines
	// acquire from the same semaphore instance.
	streamSem *semaphore.Weighted

	// ChunkSize is the raw-byte size for block-volume chunks.
	// Defaults to volume.DefaultChunkSize (256 MiB) when zero.
	// Each in-flight chunk streams to a durable on-disk .part file (not an
	// in-memory network buffer), but is read back in full from that .part file
	// for the single EncodeFrame call once complete; see Workers for the peak formula.
	ChunkSize int64

	// TTL is the DataExport TTL string (e.g. "2h").  Defaults to "2h".
	TTL string

	// RunID is an opaque per-run identifier that scopes DataExport ownership to
	// this single download run. Because the DataExport CR name is deterministic
	// (exporter.DataExportName → de-<leaf>), two concurrent runs downloading the
	// same leaf into DIFFERENT output directories resolve to the SAME CR; RunID
	// lets each run stamp the CRs it creates (exporter.WithRunOwner) and refuse to
	// delete a CR another live run owns (exporter.ReleaseDataExport), so neither
	// run tears down the other's in-flight export (inv #10b). Run generates a
	// fresh RunID via crypto/rand when it is empty; tests may set it explicitly.
	RunID string

	// KeepExports, when true, leaves the per-volume DataExport CR (and the
	// server-side export chain it owns: export VolumeSnapshot/VolumeSnapshotContent/
	// export PVC) in the cluster after each volume stream completes, instead of
	// deleting it. Zero value (false) preserves the always-delete behavior that
	// predates this field. Set from the inverse of the `--cleanup` download flag.
	KeepExports bool

	// Compression is the codec used for volume data encoding. For block volumes it
	// determines the chunk frame encoding and the output filename extension
	// (e.g. ".zst" → data.bin.zst). For filesystem volumes each file inside data.tar
	// is individually compressed with this codec: entry names become <path><ext>
	// (ext is empty for codec=none). When nil, applyDefaults creates a zstd codec.
	Compression compress.Codec

	// KubeClient performs all Kubernetes API calls.  Required.
	KubeClient client.Client

	// AggClient is the aggregated subresource API client used to fetch per-node
	// manifests via manifests-download and to resolve leaf snapshot CR group/resource
	// for DataExport targetRef. When ManifestSource is nil and AggClient is set,
	// applyDefaults builds an AggregatedManifestSource from it.
	AggClient *aggapi.Client

	// ManifestSource fetches own-scope node manifests.
	// When nil an AggregatedManifestSource backed by AggClient is used.
	ManifestSource source.ManifestSource

	// OpenExport opens a DataExport for the given snapshot leaf NodeRef and
	// returns an Export ready for data transfer. When nil SafeClient and AggClient
	// must be non-nil and the production path (exporter.OpenExport) is used.
	//
	// leafRef identifies the snapshot leaf CR to target: for CSI VolumeSnapshot
	// leaves its APIVersion/Kind are "snapshot.storage.k8s.io/v1"/"VolumeSnapshot";
	// for domain snapshot CRs they carry the domain group and kind. The DataExport
	// targetRef is derived from leafRef via the AggClient RESTMapper.
	OpenExport func(ctx context.Context, namespace string, leafRef aggapi.NodeRef, ttl string) (*exporter.Export, error)

	// SafeClient is used for DataExport HTTP connections in the production path
	// (when OpenExport is nil).  May be nil in tests that supply OpenExport.
	SafeClient *safeClient.SafeClient

	// ReadinessTimeout is how long OpenExport waits for a DataExport to become
	// Ready before returning an error.  Defaults to 5 minutes.
	ReadinessTimeout time.Duration

	// ReleaseTimeout bounds each per-volume DataExport release call (the
	// Get-before-Delete in exporter.ReleaseDataExport).  Defaults to 30 seconds.
	// downloadVolumeBinding derives a FRESH context.WithTimeout budget from this
	// value at the moment its release defer actually runs, not once up front, so
	// a slow OpenExport/WaitReady or a large volume transfer never eats into the
	// time release itself gets.
	ReleaseTimeout time.Duration

	// SelectedNodeKind and SelectedNodeName identify a single snapshot-CR node to
	// download together with its full subtree. When both are set, Run builds the
	// full tree (needed for path naming and ancestor scaffolding) and restricts
	// processing to the selected node and its descendants. Ancestor directories
	// between OutputDir and the selected node are created as content-free scaffolding
	// (no snapshot.yaml, no manifests/, no data, no sibling subtrees) so the
	// selected node sits at its real path under OutputDir.
	// When either value is empty the full tree is downloaded.
	SelectedNodeKind string
	SelectedNodeName string

	// Progress is the multi-bar progress Sink for reporting per-stream and
	// aggregate download progress. When nil, no progress output is produced
	// and download behaviour is unchanged.
	Progress progress.Sink

	// Log is the structured logger.  Defaults to slog.Default() when nil.
	Log *slog.Logger
}

// newDownloadRunID returns a fresh opaque per-run identifier used to scope
// DataExport ownership to a single download run (see Config.RunID and
// exporter.WithRunOwner). It is 16 random bytes hex-encoded (128 bits), which is
// collision-free across concurrent runs for all practical purposes.
func newDownloadRunID() (string, error) {
	var b [16]byte

	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate download run id: %w", err)
	}

	return hex.EncodeToString(b[:]), nil
}

// applyDefaults fills in zero-value Config fields with sensible defaults.
func applyDefaults(cfg Config) Config {
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}

	if cfg.PerVolumeConcurrency <= 0 {
		cfg.PerVolumeConcurrency = defaultPerVolumeConcurrency
	}

	if cfg.MaxParallelDownloads <= 0 {
		cfg.MaxParallelDownloads = defaultMaxParallelDownloads
	}

	// Create the stream semaphore once here; the pointer is shared across all
	// value-copied per-node configs so every node goroutine acquires from the same
	// semaphore instance regardless of how Config is passed down the call stack.
	cfg.streamSem = semaphore.NewWeighted(int64(cfg.MaxParallelDownloads))

	if cfg.TTL == "" {
		cfg.TTL = defaultTTL
	}

	if cfg.Compression == nil {
		codec, err := compress.New(compress.DefaultCodecName, 0)
		if err != nil {
			// DefaultCodecName is a compile-time constant; this is unreachable in production.
			panic("compress.New default codec failed: " + err.Error())
		}

		cfg.Compression = codec
	}

	if cfg.ReadinessTimeout <= 0 {
		cfg.ReadinessTimeout = defaultReadinessTimeout
	}

	if cfg.ReleaseTimeout <= 0 {
		cfg.ReleaseTimeout = defaultReleaseTimeout
	}

	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	if cfg.ManifestSource == nil && cfg.AggClient != nil {
		cfg.ManifestSource = source.NewAggregatedManifestSource(cfg.AggClient)
	}

	if cfg.OpenExport == nil && cfg.SafeClient != nil && cfg.AggClient != nil {
		sc := cfg.SafeClient
		log := cfg.Log
		c := cfg.KubeClient
		timeout := cfg.ReadinessTimeout
		aggClient := cfg.AggClient
		runID := cfg.RunID

		cfg.OpenExport = func(ctx context.Context, namespace string, leafRef aggapi.NodeRef, ttl string) (*exporter.Export, error) {
			group, resource, kind, err := aggClient.LeafDataExportTarget(leafRef)
			if err != nil {
				return nil, fmt.Errorf("resolve DataExport target for %s/%s: %w", leafRef.Kind, leafRef.Name, err)
			}

			// Stamp this run's ownership on the deterministic de-<leaf> CR BEFORE
			// OpenExport reuses it, so a concurrent run downloading the same leaf
			// into a different output dir is detected here (WARN) and its live
			// export is never deleted by this run's release (inv #10b). OpenExport's
			// own EnsureDataExport then idempotently re-fetches this stamped CR.
			owner := exporter.WithRunOwner(runID, log)
			if _, err := exporter.EnsureDataExport(ctx, c, namespace, group, resource, kind, leafRef.Name, ttl, owner); err != nil {
				return nil, fmt.Errorf("stamp DataExport ownership for %s/%s: %w", leafRef.Kind, leafRef.Name, err)
			}

			waitCtx, waitCancel := context.WithTimeout(ctx, timeout)
			defer waitCancel()

			// Pass the same ownership into OpenExport so that if the CR vanishes
			// between the stamp-Ensure above and OpenExport's inner Ensure (the
			// terminating-window recreate), the fresh CR is stamped by THIS run
			// rather than recreated unstamped (inv #10b).
			return exporter.OpenExport(waitCtx, log, c, namespace, group, resource, kind, leafRef.Name, ttl, sc, owner)
		}
	}

	return cfg
}
