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
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/snapimport"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdUse = "upload"

	flagNamespace     = "namespace"
	flagInput         = "input"
	flagNode          = "node"
	flagWorkers       = "workers"
	flagTTL           = "ttl"
	flagTimeout       = "timeout"
	flagAllowExisting = "allow-existing"
	flagTempDir       = "temp-dir"

	defaultImportWorkers = 4

	// defaultImportTTL is the default per-DataImport TTL. The DataImport TTL is an idle
	// timer (it counts down only while no bytes are being written), so it must comfortably
	// exceed the importer's provisioning + post-upload artifact-completion windows. A
	// generous default avoids spurious Expired failures on slow storage; it is far larger
	// than the data upload command's short default because a snapshot import drives a whole
	// tree of leaves end to end.
	defaultImportTTL = "1h"
)

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
  - Uploading requires RBAC to create DataImport (storage-volume-data-manager) and to call
    the manifests-and-children-refs-upload subresource (e.g. an admin kubeconfig); the
    read-only snapshot admin role is not sufficient.`,
		Example: `  # Upload the archive in ./out into namespace "restored"
  d8 snapshot upload -n restored -i ./out

  # Upload only a single VolumeSnapshot data leaf and its subtree
  d8 snapshot upload -n restored -i ./out --node VolumeSnapshot/pvc-1

  # Upload with a longer DataImport TTL and overall timeout
  d8 snapshot upload -n restored -i ./out --ttl 4h --timeout 30m`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
		},
	}

	cmd.Flags().StringP(flagNamespace, "n", "", "target namespace to import into (required)")
	cmd.Flags().StringP(flagInput, "i", "", "root archive directory produced by 'd8 snapshot download' (required)")
	cmd.Flags().String(flagNode, "", "restrict import to a single node subtree; format '<Kind>/<name>' (e.g. --node VolumeSnapshot/pvc-1)")
	cmd.Flags().Int(flagWorkers, defaultImportWorkers, "maximum number of data-leaf volume uploads to run in parallel (each worker may decompress a block volume to a temp file)")
	cmd.Flags().String(flagTTL, defaultImportTTL, "idle TTL for each data-leaf DataImport (e.g. 2h, 30m); must exceed the importer's provisioning and post-upload completion time")
	cmd.Flags().Duration(flagTimeout, 20*time.Minute, "timeout for per-node readiness/completion waits")
	cmd.Flags().Bool(flagAllowExisting, false, "downgrade namespace preflight conflict check to a warning (import-mode markers from a prior run are never conflicts regardless of this flag)")
	cmd.Flags().String(flagTempDir, "", "directory for decompressed block-volume temporary files (default: archive node directory; peak disk = --workers × largest decompressed volume)")

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

	tempDir, err := cmd.Flags().GetString(flagTempDir)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagTempDir, err)
	}

	safeClient.SupportNoAuth = false

	sc, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("building kube client: %w", err)
	}

	kubeClient, err := sc.NewRTClient(
		snapshotapi.AddToScheme,
		snapv1.AddToScheme,
	)
	if err != nil {
		return fmt.Errorf("building runtime client: %w", err)
	}

	aggClient, err := aggapi.NewClientForConfig(sc.RESTConfig(), kubeClient.RESTMapper())
	if err != nil {
		return fmt.Errorf("building aggregated API client: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(sc.RESTConfig())
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	volumes := snapimport.NewClusterVolumeImporter(dynClient, sc, ttl, timeout, 3*time.Second, tempDir, log)

	cfg := snapimport.Config{
		Namespace:        namespace,
		InputDir:         inputDir,
		SelectedNodeKind: selectedKind,
		SelectedNodeName: selectedName,
		Workers:          workers,
		AllowExisting:    allowExisting,
		TTL:              ttl,
		Timeout:          timeout,
		TempDir:          tempDir,
		Uploader:         aggClient,
		Volumes:          volumes,
		Dynamic:          dynClient,
		Mapper:           kubeClient.RESTMapper(),
		Log:              log,
	}

	log.Info("starting snapshot upload",
		slog.String("namespace", namespace),
		slog.String("input", inputDir),
	)

	if err := snapimport.Run(ctx, cfg); err != nil {
		return fmt.Errorf("snapshot upload failed: %w", err)
	}

	log.Info("snapshot upload complete", slog.String("namespace", namespace))

	return nil
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
