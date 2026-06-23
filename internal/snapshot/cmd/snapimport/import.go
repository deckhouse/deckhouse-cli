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

// Package snapimportcmd implements the `d8 snapshot import` command. The package name
// avoids the reserved word "import".
package snapimportcmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
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
	cmdUse = "import"

	flagNamespace = "namespace"
	flagInput     = "input"
	flagTTL       = "ttl"
	flagTimeout   = "timeout"

	// defaultImportTTL is the default per-DataImport TTL. The DataImport TTL is an idle
	// timer (it counts down only while no bytes are being written), so it must comfortably
	// exceed the importer's provisioning + post-upload artifact-completion windows. A
	// generous default avoids spurious Expired failures on slow storage; it is far larger
	// than the data upload command's short default because a snapshot import drives a whole
	// tree of leaves end to end.
	defaultImportTTL = "1h"
)

// NewCommand builds the `d8 snapshot import` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [flags]",
		Short:         "Import a downloaded snapshot archive into a namespace",
		SilenceUsage:  true,
		SilenceErrors: true,
		Long: `Import a local snapshot archive (produced by 'd8 snapshot download') into a namespace.

The archive tree is reconstructed in stages: first every import-mode CR is created
top-down (parents before children) so each child carries a child->parent ownerRef the
state-snapshotter binders use to attach it to its parent; then every node's manifests plus
direct child refs are uploaded to the state-snapshotter aggregated API; only after all
manifests are uploaded are the data-leaf volume bytes imported via a DataImport. Manifests
are uploaded before any volume data because a leaf's DataImport stays pending until the leaf
VolumeSnapshot is bound, which requires its ancestors' manifests to already be present.
After the whole tree is uploaded it waits for the root Snapshot and its bound SnapshotContent
to become Ready, leaving the namespace ready for 'd8 snapshot restore'.

Scope and limitations:
  - Only core Snapshot trees and CSI VolumeSnapshot data leaves can be imported client-side.
    Domain/demo snapshot nodes (e.g. intermediate DemoVirtualMachineSnapshot) expose no
    client-settable import marker and must be reconstructed by their domain controller.
  - Only block-volume data leaves are supported; filesystem-volume data import is not yet
    implemented.
  - Importing requires RBAC to create DataImport (storage-volume-data-manager) and to call
    the manifests-and-children-refs-upload subresource (e.g. an admin kubeconfig); the
    read-only snapshot admin role is not sufficient.`,
		Example: `  # Import the archive in ./out into namespace "restored"
  d8 snapshot import -n restored -i ./out

  # Import with a longer DataImport TTL and overall timeout
  d8 snapshot import -n restored -i ./out --ttl 4h --timeout 30m`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
		},
	}

	cmd.Flags().StringP(flagNamespace, "n", "", "target namespace to import into (required)")
	cmd.Flags().StringP(flagInput, "i", "", "root archive directory produced by 'd8 snapshot download' (required)")
	cmd.Flags().String(flagTTL, defaultImportTTL, "idle TTL for each data-leaf DataImport (e.g. 2h, 30m); must exceed the importer's provisioning and post-upload completion time")
	cmd.Flags().Duration(flagTimeout, 20*time.Minute, "timeout for per-node readiness/completion waits")

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

	ttl, err := cmd.Flags().GetString(flagTTL)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagTTL, err)
	}

	timeout, err := cmd.Flags().GetDuration(flagTimeout)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagTimeout, err)
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

	volumes := snapimport.NewClusterVolumeImporter(dynClient, sc, ttl, timeout, 3*time.Second, log)

	cfg := snapimport.Config{
		Namespace: namespace,
		InputDir:  inputDir,
		TTL:       ttl,
		Timeout:   timeout,
		Uploader:  aggClient,
		Volumes:   volumes,
		Dynamic:   dynClient,
		Mapper:    kubeClient.RESTMapper(),
		Log:       log,
	}

	log.Info("starting snapshot import",
		slog.String("namespace", namespace),
		slog.String("input", inputDir),
	)

	if err := snapimport.Run(ctx, cfg); err != nil {
		return fmt.Errorf("snapshot import failed: %w", err)
	}

	log.Info("snapshot import complete", slog.String("namespace", namespace))

	return nil
}
