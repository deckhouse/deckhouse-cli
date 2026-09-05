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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
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
	// For block volumes each worker finalizes up to PerVolumeConcurrency frames
	// simultaneously. Raw 256 MiB chunks remain on disk; encoding memory is
	// bounded by codec windows and buffers, independent of chunk size. zstd uses
	// one encoder worker and at most an 8 MiB window per frame plus fixed block,
	// history, and output buffers. Worst-case finalize memory therefore scales
	// with Workers × PerVolumeConcurrency × that per-frame codec bound, not
	// Workers × PerVolumeConcurrency × volume.DefaultChunkSize.
	Workers int

	// PerVolumeConcurrency is the maximum number of parallel chunk or file
	// downloads per volume (default: 4).
	// Multiplied with Workers and the codec's bounded per-frame working set it
	// determines the worst-case finalize RSS; see Workers for the peak formula.
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

	// TTL is the DataExport TTL string (e.g. "2h").  Defaults to "2h".
	TTL string

	// RunID is an opaque per-run identifier that scopes DataExport ownership to
	// this single download run. Because the DataExport CR name is deterministic,
	// two concurrent runs downloading the
	// same leaf into DIFFERENT output directories resolve to the SAME CR; RunID
	// lets each run stamp the CRs it creates (exporter.WithRunOwner) and refuse to
	// delete a CR another live run owns (exporter.ReleaseDataExport), so neither
	// run tears down the other's in-flight export (inv #10b). Run generates a
	// fresh RunID via crypto/rand when it is empty; tests may set it explicitly.
	RunID string

	// Publish routes each leaf's volume bytes through the storage-foundation-published
	// (Ingress) exporter endpoint (status.publicURL) instead of the in-cluster service
	// (status.url). It is forwarded verbatim to exporter.WithPublish; the exporter
	// derives both the DataExport spec value and the readiness/transport mode from it.
	Publish bool

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
	// returns an Export ready for data transfer. When nil TransportClient and AggClient
	// must be non-nil and the production path (exporter.OpenExport) is used.
	//
	// leafRef identifies the snapshot leaf CR to target: for CSI VolumeSnapshot
	// leaves its APIVersion/Kind are "snapshot.storage.k8s.io/v1"/"VolumeSnapshot";
	// for domain snapshot CRs they carry the domain group and kind. The DataExport
	// targetRef is derived from leafRef via the AggClient RESTMapper.
	OpenExport func(ctx context.Context, namespace string, leafRef aggapi.NodeRef, ttl string) (*exporter.Export, error)

	// OpenExportWithAcquisition opens an export and returns operation-scoped
	// cleanup evidence for the exact DataExport acquired while opening it. The
	// acquisition may be non-nil together with an error when EnsureDataExport
	// succeeded but WaitReady or transport setup failed; callers must retain it
	// so that exact object is still cleaned up. Tests and alternative transports
	// that need cleanup evidence may supply it directly; the UID-aware production
	// path uses OpenExportWithTargetAcquisition. OpenExport remains the
	// compatibility path for implementations that do not manage DataExport
	// lifecycle.
	OpenExportWithAcquisition func(
		ctx context.Context,
		namespace string,
		leafRef aggapi.NodeRef,
		ttl string,
	) (*exporter.Export, *exporter.DataExportAcquisition, error)

	// OpenExportWithTargetAcquisition is the UID-aware production lifecycle
	// callback. It carries the exact Snapshot CR UID in addition to leafRef so
	// DataExport naming and adoption cannot cross Snapshot incarnations.
	OpenExportWithTargetAcquisition func(
		ctx context.Context,
		namespace string,
		leafRef aggapi.NodeRef,
		targetUID types.UID,
		ttl string,
	) (*exporter.Export, *exporter.DataExportAcquisition, error)

	// TransportClient is used for DataExport HTTP connections in the production path
	// (when OpenExport is nil).  May be nil in tests that supply OpenExport.
	TransportClient *transport.Client

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

	if cfg.OpenExport == nil && cfg.OpenExportWithAcquisition == nil &&
		cfg.OpenExportWithTargetAcquisition == nil &&
		cfg.TransportClient != nil && cfg.AggClient != nil {
		sc := cfg.TransportClient
		log := cfg.Log
		c := cfg.KubeClient
		timeout := cfg.ReadinessTimeout
		aggClient := cfg.AggClient
		runID := cfg.RunID
		publish := cfg.Publish

		cfg.OpenExportWithTargetAcquisition = func(
			ctx context.Context,
			namespace string,
			leafRef aggapi.NodeRef,
			targetUID types.UID,
			ttl string,
		) (*exporter.Export, *exporter.DataExportAcquisition, error) {
			group, resource, kind, err := aggClient.LeafDataExportTarget(leafRef)
			if err != nil {
				return nil, nil, fmt.Errorf("resolve DataExport target for %s/%s: %w", leafRef.Kind, leafRef.Name, err)
			}

			owner := exporter.WithRunOwner(runID, log)
			target := exporter.WithTargetUID(targetUID)
			termWait := exporter.WithTerminatingWaitTimeout(timeout)
			publishOpt := exporter.WithPublish(publish)

			var acquisition *exporter.DataExportAcquisition

			waitCtx, waitCancel := context.WithTimeout(ctx, timeout)
			defer waitCancel()

			exp, openErr := exporter.OpenExport(
				waitCtx,
				log,
				c,
				namespace,
				group,
				resource,
				kind,
				leafRef.Name,
				ttl,
				sc,
				target,
				owner,
				termWait,
				publishOpt,
				exporter.WithAcquisition(&acquisition),
			)
			if openErr != nil {
				return nil, acquisition, openErr
			}

			return exp, acquisition, nil
		}
	}

	return cfg
}
