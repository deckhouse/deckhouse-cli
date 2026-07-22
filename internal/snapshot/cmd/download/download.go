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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/gofrs/flock"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/apimachinery/pkg/api/resource"

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
	flagCleanup                = "cleanup"
)

// snapshotClientQPS/snapshotClientBurst raise the kube client's rate limiter
// above client-go's built-in defaults (QPS=5, Burst=10) for the SafeClient
// this command builds — see the SetQPS call site for why. A conservative,
// well-established kubectl-style bump: enough headroom for the
// --max-parallel-downloads/--workers defaults without materially increasing
// load on a healthy API server.
const (
	snapshotClientQPS   float32 = 50
	snapshotClientBurst int     = 100
)

// NewCommand builds the `d8 snapshot download` cobra command. Per the code-style
// §4 Cobra pattern the CALLER owns the root context: it is threaded in here and
// captured by the thin RunE below, rather than recovered from cmd.Context().
func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [flags] <snapshot>",
		Short:         "Download a snapshot to a local directory tree",
		SilenceUsage:  true,
		SilenceErrors: true,
		Example: `  # Download snapshot "my-snap" from namespace "default" into directory ./out
  d8 snapshot download my-snap -n default -o out

  # Download with faster compression and more concurrent workers
  d8 snapshot download my-snap -n default -o out --workers 8 --per-volume-concurrency 8

  # Download only a single node (disk snapshot) and its subtree -- the
  # generated snapshot CR name form (e.g. DemoVirtualDiskSnapshot/nss-child-abc123)
  # still works too
  d8 snapshot download my-snap -n default -o out --node DemoVirtualDisk/bk-disk-a

  # Download only the root snapshot (equivalent to a full download)
  d8 snapshot download my-snap -n default -o out --node Snapshot/my-snap`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
	}

	cmd.Flags().StringP(flagNamespace, "n", "", "snapshot namespace (required)")
	cmd.Flags().StringP(flagOutput, "o", "", "root output directory (required)")
	cmd.Flags().String(flagNode, "", "restrict download to a single node subtree; format '<Kind>/<name>' (e.g. --node DemoVirtualDisk/bk-disk-a, --node Snapshot/my-snap); the generated snapshot CR name form (e.g. DemoVirtualDiskSnapshot/nss-child-abc) is still accepted")
	cmd.Flags().String(flagTTL, "2h", "DataExport TTL (e.g. 2h, 30m)")
	cmd.Flags().Int(flagWorkers, 4, "maximum number of nodes downloaded concurrently")
	cmd.Flags().Int(flagPerVolumeConcurrency, 4, "maximum parallel chunk/file downloads per volume")
	cmd.Flags().Int(flagMaxParallelDownloads, 5, "global cap on concurrent whole-volume-stream downloads across all nodes (independent of --workers and --per-volume-concurrency)")
	cmd.Flags().String(flagChunkSize, "", "block-volume chunk size as a resource.Quantity, e.g. 256Mi, 512Mi, 1Gi (binary Ki/Mi/Gi) or decimal 128M/1G (decimal k/M/G, note lowercase k); the 'MiB'/'MB'/'GB'/'KiB' and uppercase 'K' spellings are NOT accepted; defaults to 256Mi (min 16Mi, max 1Gi)")
	cmd.Flags().String(flagVolumeCompression, compress.DefaultCodecName,
		"volume compression codec ("+strings.Join(compress.UserSelectableNames(), ", ")+
			"); block volumes: data.bin[.<ext>]; filesystem volumes: per-file compressed entries inside an uncompressed data.tar container")
	cmd.Flags().Int(flagVolumeCompressionLevel, 0,
		"compression level for the selected codec (0 = codec default; ignored when --"+flagVolumeCompression+"=none)")

	cmd.Flags().Bool(flagCleanup, true,
		"delete the per-volume DataExport (and its server-side export chain) after each volume completes; --cleanup=false leaves them in the cluster for debugging")

	return cmd
}

// Run validates flags, builds the pipeline config, and executes the download.
// It derives a signal-cancellable context from the caller-owned ctx (threaded in
// via NewCommand, per code-style §4) so that Ctrl-C (SIGINT) and SIGTERM cleanly
// stop the download.
func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
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

	outputLock, err := acquireOutputLock(outputDir)
	if err != nil {
		return err
	}

	defer func() {
		if unlockErr := outputLock.Unlock(); unlockErr != nil {
			log.Warn("failed to release output directory lock",
				slog.String("output_dir", outputDir),
				slog.String("error", unlockErr.Error()))
		}
	}()

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

	compressionName, err := cmd.Flags().GetString(flagVolumeCompression)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagVolumeCompression, err)
	}

	compressionLevel, err := cmd.Flags().GetInt(flagVolumeCompressionLevel)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagVolumeCompressionLevel, err)
	}

	// --volume-compression-level with --volume-compression=none: accepted and
	// ignored rather than rejected. "none" has no notion of a level, and
	// silently ignoring an inapplicable flag (rather than erroring) matches
	// how the rest of this CLI treats flag combinations that simply don't
	// interact (e.g. --chunk-size has no effect on a filesystem-only volume).
	// compress.New's "none" factory already ignores the level argument, so no
	// extra branching is needed here beyond this note.
	codec, err := validateVolumeCompression(compressionName, compressionLevel)
	if err != nil {
		return err
	}

	nodeFlag, err := cmd.Flags().GetString(flagNode)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagNode, err)
	}

	selectedKind, selectedName, err := parseNodeFlag(nodeFlag)
	if err != nil {
		return fmt.Errorf("invalid --%s %q: %w", flagNode, nodeFlag, err)
	}

	cleanup, err := cmd.Flags().GetBool(flagCleanup)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagCleanup, err)
	}

	safeClient.SupportNoAuth = false

	sc, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("building kube client: %w", err)
	}

	// Raise the client-side rate limiter above client-go's built-in QPS=5/
	// Burst=10 defaults: a download opens up to --max-parallel-downloads
	// concurrent DataExport lifecycles (EnsureDataExport, WaitReady polling,
	// ReleaseDataExport) that all share this client, and several volumes can
	// complete within the same 30s cleanup window. At the old defaults, that
	// burst of concurrent Get/Delete calls could make the rate limiter's Wait
	// block past the cleanup deadline, silently leaking a DataExport even on a
	// fully successful run. Set BEFORE building kubeClient/aggClient so both
	// inherit the higher limits.
	sc.SetQPS(snapshotClientQPS, snapshotClientBurst)

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
	// progress.New defaults to progress.DirectionDownload when WithDirection is
	// omitted, so download intentionally relies on that default rather than
	// passing it explicitly (see progress.WithDirection's doc comment).
	sink := progress.New(os.Stdout, tty)

	// On a TTY we want a `docker pull`-style display: clean per-leaf bars with no
	// routine log spam interleaving them. Route the pipeline logger through the
	// sink's coordinated writer (so any line that does print appears cleanly above
	// the live bars) and raise its level to WARN, suppressing the high-frequency
	// lifecycle INFO/DEBUG lines (e.g. "waiting for DataExport to be ready",
	// "processing node", "downloading volume") during the transfer. Only WARN and
	// ERROR surface while bars are live. The non-TTY/plain path keeps full INFO
	// logging unchanged (important for CI/piped output). The command's own
	// pre-/post-bar bookend logs stay on the original logger.
	runLog := log
	if tty {
		runLog = slog.New(slog.NewTextHandler(sink.LogWriter(), &slog.HandlerOptions{Level: slog.LevelWarn}))
	}

	cfg := pipeline.Config{
		Namespace:            namespace,
		RootSnapshot:         snapshotName,
		OutputDir:            outputDir,
		Workers:              workers,
		PerVolumeConcurrency: perVolume,
		MaxParallelDownloads: maxParallel,
		ChunkSize:            chunkSize,
		TTL:                  ttl,
		KeepExports:          !cleanup,
		Compression:          codec,
		KubeClient:           kubeClient,
		AggClient:            aggClient,
		SafeClient:           sc,
		SelectedNodeKind:     selectedKind,
		SelectedNodeName:     selectedName,
		Progress:             sink,
		Log:                  runLog,
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

// downloadLockFileName is the advisory lock file created directly inside the
// output directory to serialize concurrent `d8 snapshot download` runs
// against the same tree. It is a fixed, hidden name so unrelated tooling does
// not stumble on it; it deliberately does NOT carry a ".tmp" suffix, so
// archive/resume.go's stale-*.tmp sweep never touches it, and it is not one
// of the fixed file/dir names archive.ComputeNodeChecksum reads (manifests/,
// data.bin*, data.tar, data/), so its presence never perturbs a node's
// checksum or resume classification.
const downloadLockFileName = ".d8-snapshot-download.lock"

// ErrOutputDirLocked is returned by acquireOutputLock when another process
// already holds the advisory lock on the output directory.
var ErrOutputDirLocked = errors.New("output directory is locked by another d8 snapshot download run")

// acquireOutputLock takes a non-blocking advisory exclusive lock on a fixed
// lock file inside outputDir and returns the held *flock.Flock; the caller
// must Unlock it (typically via defer) once the download finishes, fails, or
// is cancelled.
//
// The resume machinery in archive/resume.go (chunk dirs, .part files, staging
// dirs, snapshot.yaml) assumes a single writer per output tree: two
// concurrent downloads sharing those paths would race and silently corrupt
// each other's progress. Rather than block a second invocation indefinitely,
// acquireOutputLock fails fast with ErrOutputDirLocked naming the directory.
//
// Stale-lock policy: the lock is a plain flock(2) (LockFileEx on Windows, via
// gofrs/flock — cross-platform, no shell-out required). The OS releases an
// flock automatically when the holding process exits for ANY reason,
// including a hard kill or crash, so a lock FILE left behind by a dead
// process is harmless — the very next TryLock succeeds because the kernel
// already dropped the advisory lock. No separate pid/staleness bookkeeping is
// needed, and none is attempted. The lock file itself is intentionally never
// removed (removing it while another process might be mid-open/lock on the
// same path is a well-known flock TOCTOU hazard — a fresh file created at the
// same path after deletion is a different inode that a stale lock file
// deletion race lets it forget); it persists as a tiny fixture in the output
// directory.
func acquireOutputLock(outputDir string) (*flock.Flock, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating output directory %s: %w", outputDir, err)
	}

	lockPath := filepath.Join(outputDir, downloadLockFileName)

	fl := flock.New(lockPath)

	locked, err := fl.TryLock()
	if err != nil {
		return nil, fmt.Errorf("locking output directory %s: %w", outputDir, err)
	}

	if !locked {
		return nil, fmt.Errorf(
			"%w: %s (finish or stop the other run first, or choose a different --%s)",
			ErrOutputDirLocked, outputDir, flagOutput)
	}

	return fl, nil
}

// validateVolumeCompression builds the requested volume Codec, restricting the
// user-facing surface to compress.UserSelectableNames() — a narrower set than
// compress.New itself accepts (compress.New/Names() stay the full registry for
// internal consumers, e.g. decoding an existing archive written under a codec
// no longer offered to users). A name outside the allow-list is rejected here,
// at flag-validation time, with an error naming both the rejected codec and
// the currently-supported set, rather than relying on compress.New's generic
// ErrUnknownCodec message (which lists ALL registered codecs, including ones
// this command does not currently allow a user to pick).
func validateVolumeCompression(name string, level int) (compress.Codec, error) {
	if !compress.IsUserSelectable(name) {
		return nil, fmt.Errorf("--%s %q is not currently supported; supported codecs: %v",
			flagVolumeCompression, name, compress.UserSelectableNames())
	}

	codec, err := compress.New(name, level)
	if err != nil {
		return nil, fmt.Errorf("building volume codec %q: %w", name, err)
	}

	return codec, nil
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

// maxChunkSize caps --chunk-size so a huge value cannot blow the
// multiplicative memory peak documented on pipeline.Config.Workers
// (Workers × PerVolumeConcurrency × ChunkSize) and so chunk-based resume
// still lands on a reasonably fine granularity. Set as 4× the 256 MiB
// default (volume.DefaultChunkSize) — 1 GiB — a generous ceiling that keeps
// the worst-case per-chunk buffer bounded on typical hosts.
const maxChunkSize = 4 * volume.DefaultChunkSize // 1 GiB

// parseChunkSize converts a human-readable size string (e.g. "256Mi", "128M")
// into bytes using k8s.io/apimachinery's resource.ParseQuantity. Delegating to
// Quantity — a direct in-repo dependency (see internal/data/dataexport) — parses
// these unit spellings strictly and, unlike the previous hand-rolled
// suffix-stripping + fmt.Sscanf("%d") parser, REJECTS trailing/embedded garbage
// (e.g. "12x3Mi", "12 3Mi") instead of silently truncating it to a different,
// unintended size. An empty string returns 0, which the pipeline interprets as
// volume.DefaultChunkSize (256 MiB). The result must fall within
// [volume.DefaultChunkSize/16, maxChunkSize]; see maxChunkSize for the
// ceiling's rationale.
//
// Accepted suffixes are exactly resource.Quantity's: binary Ki/Mi/Gi (powers of
// 1024) and decimal k/M/G (powers of 1000; decimal kilo is lowercase "k"). This
// is a deliberate divergence from the old parser, which also accepted
// "MiB"/"MB"/"GB"/"KiB" and uppercase "K"; those are NOT Quantity suffixes and
// now error. The flag help text documents the accepted spellings.
func parseChunkSize(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}

	q, err := resource.ParseQuantity(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (use a resource.Quantity, e.g. 256Mi, 512Mi, 1Gi, or decimal 128M/1G): %w", s, err)
	}

	n := q.Value()

	if n <= 0 {
		return 0, fmt.Errorf("chunk size must be positive, got %d", n)
	}

	if n < volume.DefaultChunkSize/16 {
		return 0, fmt.Errorf("chunk size %d bytes is too small (minimum %d bytes)", n, volume.DefaultChunkSize/16)
	}

	if n > maxChunkSize {
		return 0, fmt.Errorf("chunk size %d bytes is too large (maximum %d bytes)", n, maxChunkSize)
	}

	return n, nil
}
