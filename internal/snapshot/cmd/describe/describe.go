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

	if err := treeview.Render(w, tvRoot); err != nil {
		return fmt.Errorf("rendering snapshot tree: %w", err)
	}

	return nil
}

// toTreeViewNode maps a *source.Node and its descendants into a treeview.Node tree.
//
// The node label is "<Kind>/<Name>". Volume leaf labels are derived from OwnDataRefs
// (non-aggregator domain nodes) or from Binding (orphan leaf nodes). Children are
// recursed in order.
func toTreeViewNode(n *source.Node) treeview.Node {
	tv := treeview.Node{
		Label:    n.Kind + "/" + n.Name,
		Children: make([]treeview.Node, 0, len(n.Children)),
		Volumes:  volumeLabels(n),
	}

	for _, child := range n.Children {
		tv.Children = append(tv.Children, toTreeViewNode(child))
	}

	return tv
}

// volumeLabels returns the display strings for the volume entries of n.
//
// Orphan leaf nodes (Binding != nil) yield one label: the captured PVC name.
// Non-aggregator domain nodes with OwnDataRefs yield one label per binding's target name.
// Aggregator nodes and the root node without data yield no labels.
func volumeLabels(n *source.Node) []string {
	if n.Binding != nil {
		return []string{n.Binding.Target.Name}
	}

	if len(n.OwnDataRefs) == 0 {
		return nil
	}

	labels := make([]string, 0, len(n.OwnDataRefs))

	for _, ref := range n.OwnDataRefs {
		labels = append(labels, ref.Target.Name)
	}

	return labels
}
