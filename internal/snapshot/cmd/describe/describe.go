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

// Package describe implements the `d8 snapshot describe` command.
package describe

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/treeview"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdUse        = "describe"
	flagNamespace = "namespace"

	// conditionFalse is the Ready condition's "status" value for a failed/degraded snapshot.
	conditionFalse = "False"
)

// NewCommand builds the `d8 snapshot describe` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [flags] <name>",
		Short:         "Show the snapshot hierarchy as a tree",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		Example: `  # Describe snapshot "my-snap" in namespace "default"
  d8 snapshot describe my-snap -n default

  # Describe snapshot using the kubeconfig context namespace
  d8 snapshot describe my-snap`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runE(log, cmd, args)
		},
	}

	cmd.Flags().StringP(flagNamespace, "n", "", "snapshot namespace (defaults to the kubeconfig context namespace)")

	return cmd
}

// runE validates flags, builds the kube client, and delegates to Run.
func runE(log *slog.Logger, cmd *cobra.Command, args []string) error {
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
		namespace, err = utilk8s.KubeconfigNamespace("", "")
		if err != nil {
			return fmt.Errorf("resolving namespace from kubeconfig: %w", err)
		}
	}

	snapshotName := args[0]

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

	log.Info("describing snapshot",
		slog.String("namespace", namespace),
		slog.String("snapshot", snapshotName),
	)

	return Run(ctx, kubeClient, namespace, snapshotName, cmd.OutOrStdout())
}

// Run resolves the snapshot tree and renders it to w as an ASCII tree.
//
// The kubeClient parameter is injectable so tests can supply a controller-runtime
// fake client without a real cluster.
func Run(ctx context.Context, kubeClient client.Client, namespace, name string, w io.Writer) error {
	root, err := source.BuildTree(ctx, kubeClient, namespace, name)
	if err != nil {
		return fmt.Errorf("building snapshot tree for %s/%s: %w", namespace, name, err)
	}

	tvRoot := toTreeViewNode(root)
	tvRoot.Label += degradedSuffix(root.Ready)

	if err := treeview.Render(w, tvRoot); err != nil {
		return fmt.Errorf("rendering snapshot tree: %w", err)
	}

	return nil
}

// toTreeViewNode maps a *source.Node and its descendants into a treeview.Node tree.
//
// The node label is n.DisplayLabel() — the original captured object's "<Kind>/<Name>"
// when available, falling back to the snapshot CR's own identity (see DisplayLabel's
// doc comment for the root-node exception). A node's volume label (if any) is its
// captured PVC name from status.data (Variant A, ≤1 per node). Children are recursed
// in order.
func toTreeViewNode(n *source.Node) treeview.Node {
	tv := treeview.Node{
		Label:    n.DisplayLabel(),
		Children: make([]treeview.Node, 0, len(n.Children)),
		Volumes:  volumeLabels(n),
	}

	for _, child := range n.Children {
		tv.Children = append(tv.Children, toTreeViewNode(child))
	}

	return tv
}

// degradedSuffix returns the text appended to the ROOT node's rendered label when its own
// Ready condition reports a recoverable degradation (status False, reason in
// source.DegradedReadyReasons — capture completed, data intact). It returns "" otherwise, so
// callers can unconditionally append the result. Per backlog #15 this is deliberately applied
// only to the root's label by its single call site in Run; per-node child degradation display
// is a separate, not-yet-designed concern (backlog #16).
func degradedSuffix(ready source.NodeReadyStatus) string {
	if ready.Status != conditionFalse || !source.IsDegradedReason(ready.Reason) {
		return ""
	}

	return fmt.Sprintf("  (DEGRADED: %s)", ready.Message)
}

// volumeLabels returns the display strings for the volume entries of n.
//
// A node that captured its own volume (n.Data != nil) yields one label: the captured PVC
// name (status.data.sourceRef.name). Aggregator nodes and the root node without data yield no
// labels.
func volumeLabels(n *source.Node) []string {
	if n.Data == nil {
		return nil
	}

	return []string{n.Data.SourceRef.Name}
}
