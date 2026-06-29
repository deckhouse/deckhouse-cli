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
	// Each in-flight block chunk is buffered fully in memory (raw bytes read
	// via io.ReadAll plus the encoded zstd frame); see Workers for the peak formula.
	ChunkSize int64

	// TTL is the DataExport TTL string (e.g. "2h").  Defaults to "2h".
	TTL string

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

		cfg.OpenExport = func(ctx context.Context, namespace string, leafRef aggapi.NodeRef, ttl string) (*exporter.Export, error) {
			group, kind, err := aggClient.LeafDataExportTarget(leafRef)
			if err != nil {
				return nil, fmt.Errorf("resolve DataExport target for %s/%s: %w", leafRef.Kind, leafRef.Name, err)
			}

			waitCtx, waitCancel := context.WithTimeout(ctx, timeout)
			defer waitCancel()

			return exporter.OpenExport(waitCtx, log, c, namespace, group, kind, leafRef.Name, ttl, sc)
		}
	}

	return cfg
}
