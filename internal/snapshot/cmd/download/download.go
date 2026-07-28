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
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/rest"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
	systemflags "github.com/deckhouse/deckhouse-cli/internal/system/flags"
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
	flagVolumeCompression      = "volume-compression"
	flagVolumeCompressionLevel = "volume-compression-level"
	flagCleanup                = "cleanup"
)

// snapshotClientQPS/snapshotClientBurst raise the kube client's rate limiter
// above client-go's built-in defaults (QPS=5, Burst=10) for the command-scoped
// REST configuration. A conservative,
// well-established kubectl-style bump: enough headroom for the
// --max-parallel-downloads/--workers defaults without materially increasing
// load on a healthy API server.
const (
	snapshotClientQPS   float32 = 50
	snapshotClientBurst int     = 100

	// defaultControlPlaneTimeout bounds each Kubernetes or aggregated-API request.
	defaultControlPlaneTimeout = 30 * time.Second
)

var commandRESTConfigLoader = transport.NewRESTConfig

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

  # Download only a single node (disk snapshot) and its subtree -- the
  # generated snapshot CR name form (e.g. DemoVirtualDiskSnapshot/nss-child-abc123)
  # still works too
  d8 snapshot download my-snap -n default -o out --node DemoVirtualDisk/bk-disk-a

  # Download only the root snapshot (equivalent to a full download)
  d8 snapshot download my-snap -n default -o out --node Snapshot/my-snap`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(cmd.Context(), log, cmd, args)
		},
	}

	systemflags.AddPersistentFlags(cmd)

	cmd.Flags().StringP(flagNamespace, "n", "", "snapshot namespace (required)")
	cmd.Flags().StringP(flagOutput, "o", "", "root output directory (required)")
	cmd.Flags().String(flagNode, "", "restrict download to a single node subtree; format '<Kind>/<name>' (e.g. --node DemoVirtualDisk/bk-disk-a, --node Snapshot/my-snap); the generated snapshot CR name form (e.g. DemoVirtualDiskSnapshot/nss-child-abc) is still accepted")
	cmd.Flags().String(flagTTL, "2h", "DataExport TTL (e.g. 2h, 30m)")
	cmd.Flags().Int(flagWorkers, 4, "maximum number of nodes downloaded concurrently")
	cmd.Flags().Int(flagPerVolumeConcurrency, 4, "maximum parallel chunk/file downloads per volume")
	cmd.Flags().Int(flagMaxParallelDownloads, 5, "global cap on concurrent whole-volume-stream downloads across all nodes (independent of --workers and --per-volume-concurrency)")
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
// It derives a signal-cancellable context from the Cobra execution context so
// that Ctrl-C (SIGINT) and SIGTERM cleanly stop the download.
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

	outputLock, err := acquireOutputLockContext(ctx, outputDir)
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

	destination, err := archive.NewLockedRootedDestination(outputLock, nil)
	if err != nil {
		return fmt.Errorf("open locked output destination: %w", err)
	}

	defer func() {
		if closeErr := destination.Close(); closeErr != nil {
			log.Warn("failed to close output destination",
				slog.String("output_dir", outputDir),
				slog.String("error", closeErr.Error()))
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
	// interact. compress.New's "none" factory already ignores the level
	// argument, so no extra branching is needed here beyond this note.
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

	restConfig, err := newCommandRESTConfig(cmd, commandRESTConfigLoader)
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
	kubeClient, err := transport.NewRuntimeClient(
		restConfig,
		snapshotapi.AddToScheme,
		deapi.AddToScheme,
		snapv1.AddToScheme,
	)
	if err != nil {
		return fmt.Errorf("building runtime client: %w", err)
	}

	aggClient, dataPlaneClient, err := newDownloadClients(
		restConfig,
		kubeClient.RESTMapper(),
		aggapi.NewClientForConfig,
		transport.NewClientForConfig,
	)
	if err != nil {
		return err
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
		TTL:                  ttl,
		KeepExports:          !cleanup,
		Compression:          codec,
		KubeClient:           kubeClient,
		AggClient:            aggClient,
		TransportClient:      dataPlaneClient,
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

	if err := outputLock.Verify(); err != nil {
		sink.Wait()

		return fmt.Errorf("verify locked output directory before download: %w", err)
	}

	runErr := pipeline.RunRooted(ctx, cfg, destination)

	lockErr := outputLock.Verify()
	if runErr != nil || lockErr != nil {
		sink.Wait()

		return errors.Join(
			wrapDownloadError(runErr),
			wrapOutputLockVerifyError(lockErr),
		)
	}

	sink.Wait()

	log.Info("snapshot download complete", slog.String("output_dir", outputDir))

	return nil
}

func newCommandRESTConfig(cmd *cobra.Command, load transport.RESTConfigLoader) (*rest.Config, error) {
	config, err := load(cmd.PersistentFlags())
	if err != nil {
		return nil, err
	}

	config.QPS = snapshotClientQPS
	config.Burst = snapshotClientBurst
	config.Timeout = defaultControlPlaneTimeout

	previousWrap := config.WrapTransport
	config.WrapTransport = func(roundTripper http.RoundTripper) http.RoundTripper {
		if transport, ok := roundTripper.(*http.Transport); ok {
			cloned := transport.Clone()
			cloned.TLSHandshakeTimeout = defaultControlPlaneTimeout
			cloned.ResponseHeaderTimeout = defaultControlPlaneTimeout

			baseDialContext := cloned.DialContext
			if baseDialContext == nil {
				baseDialContext = (&net.Dialer{}).DialContext
			}

			cloned.DialContext = func(
				ctx context.Context,
				network string,
				address string,
			) (net.Conn, error) {
				dialCtx, cancel := context.WithTimeout(ctx, defaultControlPlaneTimeout)
				defer cancel()

				return baseDialContext(dialCtx, network, address)
			}

			roundTripper = cloned
		}

		if previousWrap != nil {
			return previousWrap(roundTripper)
		}

		return roundTripper
	}

	return config, nil
}

type aggClientFactory func(*rest.Config, meta.RESTMapper) (*aggapi.Client, error)

type dataPlaneClientFactory func(*rest.Config) *transport.Client

func newDownloadClients(
	controlPlaneConfig *rest.Config,
	mapper meta.RESTMapper,
	newAggClient aggClientFactory,
	newDataPlaneClient dataPlaneClientFactory,
) (*aggapi.Client, *transport.Client, error) {
	aggClient, err := newAggClient(controlPlaneConfig, mapper)
	if err != nil {
		return nil, nil, fmt.Errorf("building aggregated API client: %w", err)
	}

	dataPlaneConfig := rest.CopyConfig(controlPlaneConfig)
	dataPlaneConfig.Timeout = 0

	return aggClient, newDataPlaneClient(dataPlaneConfig), nil
}

const downloadLockFileName = ".d8-snapshot-download.lock"

// ErrOutputDirLocked is returned by acquireOutputLock when another process
// already holds the advisory lock on the output directory.
var ErrOutputDirLocked = errors.New("output directory is locked by another d8 snapshot download run")

// acquireOutputLock takes a non-blocking advisory exclusive lock on a fixed
// lock file inside outputDir and returns the held archive lock; the caller
// must Unlock it (typically via defer) once the download finishes, fails, or
// is cancelled.
//
// The resume machinery in archive/resume.go (chunk dirs, .part files, staging
// dirs, snapshot.yaml) assumes a single writer per output tree: two
// concurrent downloads sharing those paths would race and silently corrupt
// each other's progress. Rather than block a second invocation indefinitely,
// acquireOutputLock fails fast with ErrOutputDirLocked naming the directory.
//
// Stale-lock policy: the lock uses flock(2) on supported Unix systems and
// LockFileEx on Windows. The OS releases an
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
func acquireOutputLock(outputDir string) (*archive.Lock, error) {
	return acquireOutputLockContext(context.Background(), outputDir)
}

func acquireOutputLockContext(ctx context.Context, outputDir string) (*archive.Lock, error) {
	lock, err := archive.AcquireWriteLockContext(ctx, outputDir)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil && errors.Is(err, ctx.Err()) {
			return nil, fmt.Errorf("locking output directory %s: %w", outputDir, cause)
		}

		if errors.Is(err, archive.ErrArchiveLocked) {
			return nil, fmt.Errorf(
				"%w: %s (finish or stop the other download/upload first, or choose a different --%s)",
				ErrOutputDirLocked, outputDir, flagOutput)
		}

		return nil, fmt.Errorf(
			"locking output directory %s: %w", outputDir, err)
	}

	return lock, nil
}

func wrapDownloadError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("snapshot download failed: %w", err)
}

func wrapOutputLockVerifyError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("verify locked output directory after download: %w", err)
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
