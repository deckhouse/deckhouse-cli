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

// Package download implements the `d8 snapshot download` command.
package download

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdUse = "download"

	flagNamespace              = "namespace"
	flagOutput                 = "output"
	flagNode                   = "node"
	flagTTL                    = "ttl"
	flagWorkers                = "workers"
	flagPerVolumeConcurrency   = "per-volume-concurrency"
	flagMaxParallelDownloads   = "max-parallel-downloads"
	flagChunkSize              = "chunk-size"
	flagVolumeCompression      = "volume-compression"
	flagVolumeCompressionLevel = "volume-compression-level"
)

// NewCommand builds the `d8 snapshot download` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [flags] <snapshot>",
		Short:         "Download a snapshot to a local directory tree",
		SilenceUsage:  true,
		SilenceErrors: true,
		Example: `  # Download snapshot "my-snap" from namespace "default" into directory ./out
  d8 snapshot download my-snap -n default -o out

  # Download with faster compression and more concurrent workers
  d8 snapshot download my-snap -n default -o out --workers 8 --per-volume-concurrency 8

  # Download block volumes with lz4 compression (faster, larger output)
  d8 snapshot download my-snap -n default -o out --volume-compression lz4

  # Download block volumes without compression
  d8 snapshot download my-snap -n default -o out --volume-compression none

  # Download only a single node (disk snapshot) and its subtree
  d8 snapshot download my-snap -n default -o out --node DemoVirtualDiskSnapshot/nss-child-abc123

  # Download only the root snapshot (equivalent to a full download)
  d8 snapshot download my-snap -n default -o out --node Snapshot/my-snap

  # Note: filesystem volumes always produce data.tar (uncompressed container);
  # file entries inside data.tar are individually compressed with the selected codec
  # (codec=none writes plain uncompressed entries)`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
		},
	}

	cmd.Flags().StringP(flagNamespace, "n", "", "snapshot namespace (required)")
	cmd.Flags().StringP(flagOutput, "o", "", "root output directory (required)")
	cmd.Flags().String(flagNode, "", "restrict download to a single node subtree; format '<Kind>/<name>' (e.g. --node DemoVirtualDiskSnapshot/nss-child-abc, --node Snapshot/my-snap)")
	cmd.Flags().String(flagTTL, "2h", "DataExport TTL (e.g. 2h, 30m)")
	cmd.Flags().Int(flagWorkers, 4, "maximum number of nodes downloaded concurrently")
	cmd.Flags().Int(flagPerVolumeConcurrency, 4, "maximum parallel chunk/file downloads per volume")
	cmd.Flags().Int(flagMaxParallelDownloads, 5, "global cap on concurrent whole-volume-stream downloads across all nodes (independent of --workers and --per-volume-concurrency)")
	cmd.Flags().String(flagChunkSize, "", "block-volume chunk size (e.g. 256Mi); defaults to 256Mi")
	cmd.Flags().String(flagVolumeCompression, compress.DefaultCodecName,
		"volume compression codec ("+strings.Join(compress.Names(), ", ")+
			"); block volumes: data.bin[.<ext>]; filesystem volumes: per-file compressed entries inside an uncompressed data.tar container")
	cmd.Flags().Int(flagVolumeCompressionLevel, 0,
		"compression level for the selected codec (0 = codec default)")

	return cmd
}

// Run validates flags, builds the pipeline config, and executes the download.
// It derives a signal-cancellable context from cmd.Context() so that Ctrl-C
// (SIGINT) and SIGTERM cleanly stop the download.
func Run(log *slog.Logger, cmd *cobra.Command, args []string) error {
	parentCtx := cmd.Context()
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	ctx, cancel := signal.NotifyContext(parentCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	namespace, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagNamespace, err)
	}

	if namespace == "" {
		return fmt.Errorf("--%s is required", flagNamespace)
	}

	snapshotName := args[0]

	outputDir, err := cmd.Flags().GetString(flagOutput)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagOutput, err)
	}

	if outputDir == "" {
		return fmt.Errorf("--%s is required", flagOutput)
	}

	outputDir, err = filepath.Abs(outputDir)
	if err != nil {
		return fmt.Errorf("resolving output path: %w", err)
	}

	ttl, err := cmd.Flags().GetString(flagTTL)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagTTL, err)
	}

	workers, err := cmd.Flags().GetInt(flagWorkers)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagWorkers, err)
	}

	perVolume, err := cmd.Flags().GetInt(flagPerVolumeConcurrency)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagPerVolumeConcurrency, err)
	}

	maxParallel, err := cmd.Flags().GetInt(flagMaxParallelDownloads)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagMaxParallelDownloads, err)
	}

	chunkSizeStr, err := cmd.Flags().GetString(flagChunkSize)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagChunkSize, err)
	}

	chunkSize, err := parseChunkSize(chunkSizeStr)
	if err != nil {
		return fmt.Errorf("parsing --%s: %w", flagChunkSize, err)
	}

	codecName, err := cmd.Flags().GetString(flagVolumeCompression)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagVolumeCompression, err)
	}

	compressionLevel, err := cmd.Flags().GetInt(flagVolumeCompressionLevel)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagVolumeCompressionLevel, err)
	}

	codec, err := compress.New(codecName, compressionLevel)
	if err != nil {
		return fmt.Errorf("invalid --%s %q (valid codecs: %s): %w",
			flagVolumeCompression, codecName, strings.Join(compress.Names(), ", "), err)
	}

	nodeFlag, err := cmd.Flags().GetString(flagNode)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagNode, err)
	}

	selectedKind, selectedName, err := parseNodeFlag(nodeFlag)
	if err != nil {
		return fmt.Errorf("invalid --%s %q: %w", flagNode, nodeFlag, err)
	}

	safeClient.SupportNoAuth = false

	sc, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("building kube client: %w", err)
	}

	kubeClient, err := sc.NewRTClient(
		snapshotapi.AddToScheme,
		deapi.AddToScheme,
		snapv1.AddToScheme,
	)
	if err != nil {
		return fmt.Errorf("building runtime client: %w", err)
	}

	aggClient, err := aggapi.NewClientForConfig(sc.RESTConfig(), kubeClient.RESTMapper())
	if err != nil {
		return fmt.Errorf("building aggregated API client: %w", err)
	}

	tty := term.IsTerminal(int(os.Stdout.Fd()))
	sink := progress.New(os.Stdout, tty)

	cfg := pipeline.Config{
		Namespace:            namespace,
		RootSnapshot:         snapshotName,
		OutputDir:            outputDir,
		Workers:              workers,
		PerVolumeConcurrency: perVolume,
		MaxParallelDownloads: maxParallel,
		ChunkSize:            chunkSize,
		TTL:                  ttl,
		Compression:          codec,
		KubeClient:           kubeClient,
		AggClient:            aggClient,
		SafeClient:           sc,
		SelectedNodeKind:     selectedKind,
		SelectedNodeName:     selectedName,
		Progress:             sink,
		Log:                  log,
	}

	log.Info("starting snapshot download",
		slog.String("namespace", namespace),
		slog.String("snapshot", snapshotName),
		slog.String("output_dir", outputDir),
	)

	if err := pipeline.Run(ctx, cfg); err != nil {
		sink.Wait()

		return fmt.Errorf("snapshot download failed: %w", err)
	}

	sink.Wait()

	log.Info("snapshot download complete", slog.String("output_dir", outputDir))

	return nil
}

// parseNodeFlag parses a --node flag value "<Kind>/<name>" into its components.
// An empty string returns empty strings and no error (full-tree download).
// The value must contain exactly one "/" with a non-empty kind and name on each side.
func parseNodeFlag(s string) (string, string, error) {
	if s == "" {
		return "", "", nil
	}

	idx := strings.IndexByte(s, '/')
	if idx < 0 {
		return "", "", fmt.Errorf("expected format '<Kind>/<name>', got %q: missing '/'", s)
	}

	kind := s[:idx]
	name := s[idx+1:]

	if kind == "" {
		return "", "", fmt.Errorf("kind must not be empty in %q", s)
	}

	if name == "" {
		return "", "", fmt.Errorf("name must not be empty in %q", s)
	}

	if strings.Contains(name, "/") {
		return "", "", fmt.Errorf("name must not contain '/' in %q; expected exactly one '/'", s)
	}

	return kind, name, nil
}

// parseChunkSize converts a human-readable size string (e.g. "256Mi", "128M")
// into bytes. An empty string returns 0, which the pipeline interprets as
// volume.DefaultChunkSize (256 MiB).
func parseChunkSize(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}

	// Normalise "Mi" -> "MiB" style variants for resource.Quantity.
	s = strings.TrimSpace(s)

	var mult int64 = 1

	switch {
	case strings.HasSuffix(s, "GiB") || strings.HasSuffix(s, "Gi"):
		mult = 1024 * 1024 * 1024
		s = strings.TrimRightFunc(s, func(r rune) bool { return r == 'B' || r == 'i' || r == 'G' })
	case strings.HasSuffix(s, "MiB") || strings.HasSuffix(s, "Mi"):
		mult = 1024 * 1024
		s = strings.TrimRightFunc(s, func(r rune) bool { return r == 'B' || r == 'i' || r == 'M' })
	case strings.HasSuffix(s, "KiB") || strings.HasSuffix(s, "Ki"):
		mult = 1024
		s = strings.TrimRightFunc(s, func(r rune) bool { return r == 'B' || r == 'i' || r == 'K' })
	case strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G"):
		mult = 1000 * 1000 * 1000
		s = strings.TrimRightFunc(s, func(r rune) bool { return r == 'B' || r == 'G' })
	case strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M"):
		mult = 1000 * 1000
		s = strings.TrimRightFunc(s, func(r rune) bool { return r == 'B' || r == 'M' })
	case strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K"):
		mult = 1000
		s = strings.TrimRightFunc(s, func(r rune) bool { return r == 'B' || r == 'K' })
	}

	var n int64

	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}

	if n <= 0 {
		return 0, fmt.Errorf("chunk size must be positive, got %d", n)
	}

	result := n * mult
	if result < volume.DefaultChunkSize/16 {
		return 0, fmt.Errorf("chunk size %d bytes is too small (minimum %d bytes)", result, volume.DefaultChunkSize/16)
	}

	return result, nil
}
