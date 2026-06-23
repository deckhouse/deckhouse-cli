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

// Package restorecmd implements the `d8 snapshot restore` command.
package restorecmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/spf13/cobra"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/restore"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdUse = "restore"

	flagNamespace = "namespace"
	flagWait      = "wait"
	flagTimeout   = "timeout"
)

// NewCommand builds the `d8 snapshot restore` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [flags] <snapshot>",
		Short:         "Restore a snapshot in-place into its namespace",
		SilenceUsage:  true,
		SilenceErrors: true,
		Long: `Restore a snapshot tree in-namespace.

The snapshot is restored into the same namespace it lives in: -n/--namespace is both
the source Snapshot namespace and the restore target. The required VolumeSnapshot /
VirtualDiskSnapshot leaves must already be present in that namespace (they exist after
the original snapshot, or after the snapshot is re-created in this namespace).

Cross-namespace restore is not a single command: it is a separate procedure that downloads
the snapshot from the source namespace, re-creates the Snapshot and its volume-snapshot
leaves in the target namespace, and then runs restore there.

The server compiles the whole subtree in one call; every returned object is applied as-is.
PersistentVolumeClaims already carry spec.dataSourceRef pointing at the VolumeSnapshot (or
VirtualDiskSnapshot for domain disks) present in the namespace, so CSI provisions the data.

--wait only tracks PersistentVolumeClaims that appear in the restored manifest set. Disk-backed
PVCs for domain objects are recreated asynchronously by the domain controller (not part of this
output), so they are not awaited; the command may return before such volumes finish provisioning.`,
		Example: `  # Restore snapshot "my-snap" in namespace "default"
  d8 snapshot restore my-snap -n default

  # Restore and wait for the restored PVCs to become Bound
  d8 snapshot restore my-snap -n default --wait --timeout 15m`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
		},
	}

	cmd.Flags().StringP(flagNamespace, "n", "", "snapshot namespace; also the restore target namespace (required)")
	cmd.Flags().Bool(flagWait, false, "wait for restored PersistentVolumeClaims to become Bound (only PVCs in the manifest set; domain disk-backed PVCs created asynchronously are not awaited)")
	cmd.Flags().Duration(flagTimeout, 10*time.Minute, "timeout for the --wait Bound check")

	return cmd
}

// Run validates flags, builds the kube clients, and executes the restore.
// It derives a signal-cancellable context from cmd.Context() so that Ctrl-C
// (SIGINT) and SIGTERM cleanly stop the restore.
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

	wait, err := cmd.Flags().GetBool(flagWait)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagWait, err)
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

	cfg := restore.Config{
		Namespace: namespace,
		Snapshot:  snapshotName,
		Wait:      wait,
		Timeout:   timeout,
		Source:    aggClient,
		Dynamic:   dynClient,
		Mapper:    kubeClient.RESTMapper(),
		Log:       log,
	}

	log.Info("starting snapshot restore",
		slog.String("namespace", namespace),
		slog.String("snapshot", snapshotName),
	)

	if err := restore.Run(ctx, cfg); err != nil {
		return fmt.Errorf("snapshot restore failed: %w", err)
	}

	log.Info("snapshot restore complete",
		slog.String("namespace", namespace),
		slog.String("snapshot", snapshotName),
	)

	return nil
}
