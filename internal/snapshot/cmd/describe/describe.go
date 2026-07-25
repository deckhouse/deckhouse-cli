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
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/treeview"
	systemflags "github.com/deckhouse/deckhouse-cli/internal/system/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const (
	cmdUse = "describe"

	flagNamespace  = "namespace"
	flagKubeconfig = "kubeconfig"
	flagContext    = "context"

	// defaultControlPlaneTimeout bounds each Kubernetes control-plane request.
	defaultControlPlaneTimeout = 30 * time.Second

	// conditionFalse is the Ready condition's "status" value for a failed/degraded snapshot.
	conditionFalse = "False"
)

var commandRESTConfigLoader = transport.NewRESTConfig

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

	systemflags.AddPersistentFlags(cmd)

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

	namespace, err := resolveCommandNamespace(cmd)
	if err != nil {
		return err
	}

	snapshotName := args[0]

	restConfig, err := newCommandRESTConfig(cmd, commandRESTConfigLoader)
	if err != nil {
		return fmt.Errorf("building kube client: %w", err)
	}

	kubeClient, err := transport.NewRuntimeClient(
		restConfig,
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

func resolveCommandNamespace(cmd *cobra.Command) (string, error) {
	namespace, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		return "", fmt.Errorf("reading --%s flag: %w", flagNamespace, err)
	}

	if namespace != "" {
		return namespace, nil
	}

	kubeconfigPath, err := cmd.PersistentFlags().GetString(flagKubeconfig)
	if err != nil {
		return "", fmt.Errorf("reading --%s flag: %w", flagKubeconfig, err)
	}

	contextName, err := cmd.PersistentFlags().GetString(flagContext)
	if err != nil {
		return "", fmt.Errorf("reading --%s flag: %w", flagContext, err)
	}

	namespace, err = utilk8s.KubeconfigNamespace(kubeconfigPath, contextName)
	if err != nil {
		return "", fmt.Errorf("resolving namespace from kubeconfig: %w", err)
	}

	return namespace, nil
}

func newCommandRESTConfig(cmd *cobra.Command, load transport.RESTConfigLoader) (*rest.Config, error) {
	config, err := load(cmd.PersistentFlags())
	if err != nil {
		return nil, err
	}

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
