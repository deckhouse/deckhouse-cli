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

// Package snapimportcmd implements the `d8 snapshot upload` command. The package name
// avoids the reserved word "import".
package snapimportcmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/snapimport"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
	systemflags "github.com/deckhouse/deckhouse-cli/internal/system/flags"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdUse = "upload"

	flagNamespace                  = "namespace"
	flagInput                      = "input"
	flagNode                       = "node"
	flagWorkers                    = "workers"
	flagTTL                        = "ttl"
	flagTimeout                    = "timeout"
	flagAllowExisting              = "allow-existing"
	flagAllowUnauthenticatedLegacy = "allow-unauthenticated-legacy"
	flagSkipUnsupportedFSEntries   = "skip-unsupported-fs-entries"
	flagPublish                    = "publish"

	defaultImportWorkers = 5

	// defaultImportTTL is the default per-DataImport TTL. The DataImport TTL is an idle
	// timer (it counts down only while no bytes are being written), so it must comfortably
	// exceed the importer's provisioning + post-upload artifact-completion windows. A
	// generous default avoids spurious Expired failures on slow storage; it is far larger
	// than the data upload command's short default because a snapshot import drives a whole
	// tree of leaves end to end.
	defaultImportTTL = "1h"
)

// snapshotClientQPS/snapshotClientBurst raise the kube client's rate limiter
// above client-go's built-in defaults (QPS=5, Burst=10) for the command-scoped
// REST configuration. Mirrors the same
// pinned values used by `d8 snapshot download`.
const (
	snapshotClientQPS   float32 = 50
	snapshotClientBurst int     = 100
)

var commandRESTConfigLoader = transport.NewRESTConfig

// NewCommand builds the `d8 snapshot upload` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [flags]",
		Aliases:       []string{"import"},
		Short:         "Upload a downloaded snapshot archive into a namespace",
		SilenceUsage:  true,
		SilenceErrors: true,
		Long: `Upload a local snapshot archive (produced by 'd8 snapshot download') into a namespace.

The archive tree is reconstructed in stages: first every import-mode CR is created
top-down (parents before children) so each child carries a child->parent ownerRef the
state-snapshotter binders use to attach it to its parent; then every node's manifests plus
direct child refs are uploaded to the state-snapshotter aggregated API; only after all
manifests are uploaded are the data-leaf volume bytes uploaded via a DataImport. Manifests
are uploaded before any volume data because a leaf's DataImport stays pending until the leaf
VolumeSnapshot is bound, which requires its ancestors' manifests to already be present.
After the whole tree is uploaded it waits for the root Snapshot and its bound SnapshotContent
to become Ready, leaving the namespace ready for 'd8 snapshot restore'.

--node restricts the upload to a single node and its descendants. The selected node becomes
the upload root; it must be a core Snapshot, a CSI VolumeSnapshot data leaf, or a domain
data leaf (e.g. DemoVirtualDiskSnapshot). Domain aggregator nodes (a DemoVirtualMachineSnapshot
that references child snapshots) and manifest-only domain nodes cannot be selected as the
upload root (they have no parent SnapshotContent to attach to when uploaded standalone); they
are uploaded only as part of a full-archive upload (omit --node) or by selecting an ancestor
Snapshot.

Scope and limitations:
  - Full Snapshot trees are uploaded as-is, including domain aggregator nodes (a
    DemoVirtualMachineSnapshot that aggregates child DemoVirtualDiskSnapshot nodes): the CLI
    creates each node's unified import marker and uploads its manifests + child refs, and the
    server-side genericbinder reconstructs the aggregator's content from its children's
    SnapshotContents. The aggregator is a non-root node in this case, so no DataImport is
    created for it (only its data-leaf descendants stream volume bytes).
  - Manifest-only domain nodes (e.g. a disk-less DemoVirtualMachineSnapshot, which carries
    only manifests) are uploaded as part of their parent tree (the server materialises their
    content from the uploaded manifests alone, with no data leg).
  - A domain aggregator can be reconstructed only within a tree, never as a standalone --node
    root. To upload an individual disk snapshot from such a tree on its own, use
    --node <DomainDataLeafKind>/<name> (e.g. --node DemoVirtualDiskSnapshot/dvd-1).
  - Both block-volume and filesystem-volume data leaves are supported.
  - Filesystem archives can upload regular files only. Symlink, hardlink, device, FIFO, and
    socket entries, plus empty directories other than well-known filesystem-reserved names
    (for example lost+found), cannot be uploaded: the protocol supports only regular-file
    PUTs, and creates directories only as a side effect of a file PUT inside them.
  - --skip-unsupported-fs-entries uploads the supported regular files instead of failing,
    but causes data loss for each skipped path; review the bounded post-upload summary.
  - Legacy archives without formatVersion and metadataChecksum are rejected by default because
    their snapshot.yaml identity and volume metadata are unauthenticated. Use
    --allow-unauthenticated-legacy only for deliberate migration or inspection of trusted
    pre-version archives; this mode cannot distinguish a genuine legacy archive from a
    downgraded and tampered current archive.
  - Uploading requires RBAC to create DataImport (storage-volume-data-manager) and to call
    the manifests-and-children-refs-upload subresource (e.g. an admin kubeconfig); the
    read-only snapshot admin role is not sufficient.

--publish selects how each data leaf's bytes are streamed to its DataImport importer pod.
With --publish=false (or when autodetection picks it), bytes go straight to the importer's
in-cluster service, trusting only its internal CA (status.ca). With --publish=true, bytes go
through the storage-foundation-published Ingress endpoint (status.publicURL) instead, so a
kubeconfig without direct network access to the cluster's internal service network can still
upload. If --publish is not given, the command probes whether the in-cluster importer endpoint
is reachable and picks accordingly. IMPORTANT: the publish path works only with a kubeconfig
authenticated by a bearer token. Ingress terminates TLS with its own certificate and does not
forward the client's TLS certificate to the importer pod, so a certificate-based kubeconfig
receives a 401 when --publish=true.`,
		Example: `  # Upload the archive in ./out into namespace "restored"
  d8 snapshot upload -n restored -i ./out

  # Upload only a single VolumeSnapshot data leaf and its subtree
  d8 snapshot upload -n restored -i ./out --node VolumeSnapshot/pvc-1

  # Upload with a longer DataImport TTL and overall timeout
  d8 snapshot upload -n restored -i ./out --ttl 4h --timeout 30m

  # Upload through the published Ingress endpoint (requires a bearer-token kubeconfig)
  d8 snapshot upload -n restored -i ./out --publish=true`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
		},
	}

	systemflags.AddPersistentFlags(cmd)

	cmd.Flags().StringP(flagNamespace, "n", "", "target namespace to import into (required)")
	cmd.Flags().StringP(flagInput, "i", "", "root archive directory produced by 'd8 snapshot download' (required)")
	cmd.Flags().String(flagNode, "", "restrict import to a single node subtree; format '<Kind>/<name>' (e.g. --node VolumeSnapshot/pvc-1)")
	cmd.Flags().Int(flagWorkers, defaultImportWorkers, "maximum number of data-leaf volume uploads to run in parallel (fixed cap via errgroup.SetLimit; default 5)")
	cmd.Flags().String(flagTTL, defaultImportTTL, "idle TTL for each data-leaf DataImport (e.g. 2h, 30m); must exceed the importer's provisioning and post-upload completion time")
	cmd.Flags().Duration(flagTimeout, 20*time.Minute, "timeout for per-node readiness/completion waits")
	cmd.Flags().Bool(flagAllowExisting, false, "downgrade namespace preflight conflict check to a warning (import-mode markers from a prior run are never conflicts regardless of this flag)")
	cmd.Flags().Bool(flagAllowUnauthenticatedLegacy, false, "allow trusted pre-version archives whose snapshot.yaml metadata is unauthenticated (unsafe; explicit compatibility mode)")
	cmd.Flags().Bool(flagSkipUnsupportedFSEntries, false, "skip unsupported filesystem entries and report them after upload (causes data loss for skipped paths)")
	cmd.Flags().Bool(flagPublish, false, "upload volume data through the published (ingress) importer endpoint instead of the in-cluster service; "+
		"if unset, the in-cluster endpoint's reachability is auto-detected")

	return cmd
}

// Run validates flags, builds the kube clients, and executes the import.
func Run(log *slog.Logger, cmd *cobra.Command, _ []string) error {
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

	inputDir, err := cmd.Flags().GetString(flagInput)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagInput, err)
	}

	if inputDir == "" {
		return fmt.Errorf("--%s is required", flagInput)
	}

	inputDir, err = filepath.Abs(inputDir)
	if err != nil {
		return fmt.Errorf("resolving input path: %w", err)
	}

	nodeFlag, err := cmd.Flags().GetString(flagNode)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagNode, err)
	}

	selectedKind, selectedName, err := parseNodeFlag(nodeFlag)
	if err != nil {
		return fmt.Errorf("invalid --%s %q: %w", flagNode, nodeFlag, err)
	}

	workers, err := cmd.Flags().GetInt(flagWorkers)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagWorkers, err)
	}

	ttl, err := cmd.Flags().GetString(flagTTL)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagTTL, err)
	}

	timeout, err := cmd.Flags().GetDuration(flagTimeout)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagTimeout, err)
	}

	allowExisting, err := cmd.Flags().GetBool(flagAllowExisting)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagAllowExisting, err)
	}

	allowUnauthenticatedLegacy, err := cmd.Flags().GetBool(flagAllowUnauthenticatedLegacy)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagAllowUnauthenticatedLegacy, err)
	}

	if allowUnauthenticatedLegacy {
		log.Warn("allowing unauthenticated legacy snapshot metadata",
			slog.String("warning",
				"legacy compatibility cannot distinguish a genuine pre-version archive from a downgraded tampered archive"))
	}

	skipUnsupportedFSEntries, err := cmd.Flags().GetBool(flagSkipUnsupportedFSEntries)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagSkipUnsupportedFSEntries, err)
	}

	if skipUnsupportedFSEntries {
		ctx = snapimport.WithSkipUnsupportedFSEntries(ctx)
	}

	restConfig, err := newCommandRESTConfig(cmd, commandRESTConfigLoader)
	if err != nil {
		return fmt.Errorf("building kube client: %w", err)
	}

	// Raise the client-side rate limiter above client-go's built-in QPS=5/
	// Burst=10 defaults: an upload opens up to --workers concurrent
	// DataImport lifecycles that all share this client, and several leaves can
	// finish within the same short window. At the old defaults a burst of
	// concurrent requests could make the rate limiter's Wait block long enough
	// to fail a cleanup/status call. Set BEFORE building kubeClient/aggClient/
	// dynClient so all three inherit the higher limits.
	kubeClient, err := transport.NewRuntimeClient(
		restConfig,
		snapshotapi.AddToScheme,
		snapv1.AddToScheme,
	)
	if err != nil {
		return fmt.Errorf("building runtime client: %w", err)
	}

	aggClient, err := aggapi.NewClientForConfig(restConfig, kubeClient.RESTMapper())
	if err != nil {
		return fmt.Errorf("building aggregated API client: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	dataPlaneClient := transport.NewClientForConfig(restConfig)

	publishFlag, err := dataio.ParsePublishFlag(cmd.Flags())
	if err != nil {
		return fmt.Errorf("resolving --%s: %w", flagPublish, err)
	}

	// Probe from the command's already-resolved restConfig, not a fresh parse of
	// --kubeconfig/--context: reparsing could target a different cluster than the one this
	// command is actually uploading into.
	probeClient := safeClient.NewSafeClientForConfig(restConfig)

	publish, err := dataio.ResolvePublish(ctx, publishFlag, kubeClient, probeClient, log)
	if err != nil {
		return fmt.Errorf("resolving --%s: %w", flagPublish, err)
	}

	isTTY := term.IsTerminal(int(os.Stdout.Fd()))

	// Upload shows Upload/Uploading/DataImport wording instead of progress.New's
	// default download-flavored words (progress.DirectionDownload).
	sink := progress.New(os.Stdout, isTTY, progress.WithDirection(progress.DirectionUpload))

	// On a TTY we want a `docker pull`-style display: clean per-leaf bars with no
	// routine log spam interleaving them. Route the importer/pipeline logger
	// through the sink's coordinated writer (so any line that does print appears
	// cleanly above the live bars) and raise its level to WARN, suppressing the
	// high-frequency lifecycle INFO/DEBUG lines during the transfer. Only WARN and
	// ERROR surface while bars are live. The non-TTY/plain path keeps full INFO
	// logging unchanged (important for CI/piped output). The command's own bookend
	// logs stay on the original logger.
	runLog := log
	if isTTY {
		runLog = slog.New(slog.NewTextHandler(sink.LogWriter(), &slog.HandlerOptions{Level: slog.LevelWarn}))
	}

	volumes := snapimport.NewClusterVolumeImporter(snapimport.ClusterVolumeImporterOptions{
		Dynamic:   dynClient,
		Transport: dataPlaneClient,
		TTL:       ttl,
		Publish:   publish,
		Wait:      timeout,
		Poll:      3 * time.Second,
		Log:       runLog,
	})

	cfg := snapimport.Config{
		Namespace:                  namespace,
		InputDir:                   inputDir,
		SelectedNodeKind:           selectedKind,
		SelectedNodeName:           selectedName,
		Workers:                    workers,
		AllowExisting:              allowExisting,
		AllowUnauthenticatedLegacy: allowUnauthenticatedLegacy,
		TTL:                        ttl,
		Timeout:                    timeout,
		ControlRequestTimeout:      snapimport.DefaultControlRequestTimeout,
		Uploader:                   aggClient,
		Volumes:                    volumes,
		Dynamic:                    dynClient,
		Mapper:                     kubeClient.RESTMapper(),
		Log:                        runLog,
		Progress:                   sink,
	}

	log.Info("starting snapshot upload",
		slog.String("namespace", namespace),
		slog.String("input", inputDir),
	)

	if err := snapimport.Run(ctx, cfg); err != nil {
		sink.Wait()

		return fmt.Errorf("snapshot upload failed: %w", err)
	}

	sink.Wait()

	log.Info("snapshot upload complete", slog.String("namespace", namespace))

	return nil
}

func newCommandRESTConfig(cmd *cobra.Command, load transport.RESTConfigLoader) (*rest.Config, error) {
	config, err := load(cmd.PersistentFlags())
	if err != nil {
		return nil, err
	}

	config.QPS = snapshotClientQPS
	config.Burst = snapshotClientBurst
	config.Timeout = snapimport.DefaultControlRequestTimeout

	return config, nil
}

// parseNodeFlag parses a --node flag value "<Kind>/<name>" into its components.
// An empty string returns empty strings and no error (full-archive import).
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
