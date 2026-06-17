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
	"log/slog"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	defaultWorkers              = 4
	defaultPerVolumeConcurrency = 4
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

	// ChunkSize is the raw-byte size for block-volume chunks.
	// Defaults to volume.DefaultChunkSize (256 MiB) when zero.
	// Each in-flight block chunk is buffered fully in memory (raw bytes read
	// via io.ReadAll plus the encoded zstd frame); see Workers for the peak formula.
	ChunkSize int64

	// TTL is the DataExport TTL string (e.g. "2h").  Defaults to "2h".
	TTL string

	// ZstdLevel is the zstd compression level.  Defaults to compress.LevelDefault.
	ZstdLevel compress.Level

	// KubeClient performs all Kubernetes API calls.  Required.
	KubeClient client.Client

	// ManifestSource fetches own-scope manifest chunks.
	// When nil a KubeManifestSource backed by KubeClient is used.
	ManifestSource source.ManifestSource

	// OpenExport opens a DataExport for the given shadow VolumeSnapshot name and
	// returns an Export ready for data transfer.  When nil SafeClient must be
	// non-nil and the production path (exporter.OpenExport) is used.
	OpenExport func(ctx context.Context, namespace, shadowVSName, ttl string) (*exporter.Export, error)

	// SafeClient is used for DataExport HTTP connections in the production path
	// (when OpenExport is nil).  May be nil in tests that supply OpenExport.
	SafeClient *safeClient.SafeClient

	// ReadinessTimeout is how long OpenExport waits for a DataExport to become
	// Ready before returning an error.  Defaults to 5 minutes.
	ReadinessTimeout time.Duration

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

	if cfg.TTL == "" {
		cfg.TTL = defaultTTL
	}

	if cfg.ZstdLevel == 0 {
		cfg.ZstdLevel = compress.LevelDefault
	}

	if cfg.ReadinessTimeout <= 0 {
		cfg.ReadinessTimeout = defaultReadinessTimeout
	}

	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	if cfg.ManifestSource == nil && cfg.KubeClient != nil {
		cfg.ManifestSource = source.NewKubeManifestSource(cfg.KubeClient)
	}

	if cfg.OpenExport == nil && cfg.SafeClient != nil {
		sc := cfg.SafeClient
		log := cfg.Log
		c := cfg.KubeClient
		timeout := cfg.ReadinessTimeout
		cfg.OpenExport = func(ctx context.Context, namespace, shadowVSName, ttl string) (*exporter.Export, error) {
			waitCtx, waitCancel := context.WithTimeout(ctx, timeout)
			defer waitCancel()

			return exporter.OpenExport(waitCtx, log, c, namespace, shadowVSName, ttl, sc)
		}
	}

	return cfg
}
