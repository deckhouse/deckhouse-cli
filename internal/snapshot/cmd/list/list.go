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

// Package list implements the `d8 snapshot get` command.
package list

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/dynamic"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const (
	cmdUse = "get"

	flagNamespace     = "namespace"
	flagAllNamespaces = "all-namespaces"
	flagOutput        = "output"
	flagKubeconfig    = "kubeconfig"
	flagContext       = "context"

	// readyConditionType is the status condition reporting overall Snapshot
	// readiness; it matches the type used by the restore preflight.
	readyConditionType = "Ready"

	// notAvailable is the placeholder for an empty/missing table cell, matching
	// kubectl's "<none>"-style dash.
	notAvailable = "-"
)

// errUnsupportedFormat is returned for an unknown -o value. Callers wrap it
// with the accepted formats; tests use errors.Is.
var errUnsupportedFormat = errors.New("unsupported output format")

// snapshotGVR is the dynamic resource for state-snapshotter.deckhouse.io Snapshots.
var snapshotGVR = schema.GroupVersionResource{
	Group:    snapshotapi.StorageGroup,
	Version:  snapshotapi.Version,
	Resource: "snapshots",
}

// NewCommand builds the `d8 snapshot get` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse,
		Aliases:       []string{"list", "ls"},
		Short:         "List Snapshot resources in the cluster",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		Example: `  # List snapshots in the kubeconfig context namespace
  d8 snapshot get

  # List snapshots in a specific namespace
  d8 snapshot get -n my-namespace

  # List snapshots across all namespaces
  d8 snapshot get -A

  # Machine-readable output
  d8 snapshot get -o json
  d8 snapshot get -o yaml`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return Run(log, cmd)
		},
	}

	// Reuse the standard kubeconfig/context flags (same as `d8 system ...`),
	// so NewDynamicClient and KubeconfigNamespace can read them.
	flags.AddPersistentFlags(cmd)

	cmd.Flags().StringP(flagNamespace, "n", "", "namespace to list Snapshots from (defaults to the kubeconfig context namespace)")
	cmd.Flags().BoolP(flagAllNamespaces, "A", false, "list Snapshots across all namespaces")
	utilk8s.AddOutputFlag(cmd, "table", "table", "json", "yaml")

	_ = cmd.RegisterFlagCompletionFunc(flagNamespace, func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return utilk8s.CompleteNamespaces(cmd, toComplete)
	})

	return cmd
}

// Run resolves the target namespace (kubectl-style), lists Snapshot objects and
// renders them in the requested format.
func Run(log *slog.Logger, cmd *cobra.Command) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	outputFmt, err := cmd.Flags().GetString(flagOutput)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagOutput, err)
	}

	namespace, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagNamespace, err)
	}

	allNamespaces, err := cmd.Flags().GetBool(flagAllNamespaces)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagAllNamespaces, err)
	}

	if allNamespaces && namespace != "" {
		return fmt.Errorf("--%s and --%s are mutually exclusive", flagAllNamespaces, flagNamespace)
	}

	// kubectl-style default: when neither -A nor -n is given, fall back to the
	// namespace pinned by the current kubeconfig context (or "default").
	if !allNamespaces && namespace == "" {
		kubeconfigPath, _ := cmd.Flags().GetString(flagKubeconfig)
		contextName, _ := cmd.Flags().GetString(flagContext)

		namespace, err = utilk8s.KubeconfigNamespace(kubeconfigPath, contextName)
		if err != nil {
			return err
		}
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	log.Debug("listing snapshots",
		slog.String("namespace", namespace),
		slog.Bool("all_namespaces", allNamespaces),
	)

	list, err := listSnapshots(ctx, dyn, namespace, allNamespaces)
	if err != nil {
		return err
	}

	return render(cmd.OutOrStdout(), list, allNamespaces, outputFmt)
}

// listSnapshots fetches Snapshot objects for a single namespace, or across all
// namespaces when allNamespaces is set.
func listSnapshots(ctx context.Context, dyn dynamic.Interface, namespace string, allNamespaces bool) (*unstructured.UnstructuredList, error) {
	ri := dyn.Resource(snapshotGVR)

	var (
		list *unstructured.UnstructuredList
		err  error
	)

	if allNamespaces {
		list, err = ri.List(ctx, metav1.ListOptions{})
	} else {
		list, err = ri.Namespace(namespace).List(ctx, metav1.ListOptions{})
	}

	if err != nil {
		return nil, fmt.Errorf("listing Snapshots: %w", err)
	}

	return list, nil
}

// render dispatches the list to the requested output format. json/yaml emit the
// raw Snapshot objects (full fidelity); table renders the summary columns.
func render(w io.Writer, list *unstructured.UnstructuredList, allNamespaces bool, outputFmt string) error {
	switch outputFmt {
	case "json", "yaml":
		return utilk8s.PrintObject(w, &unstructured.Unstructured{Object: list.UnstructuredContent()}, outputFmt)
	case "table", "":
		return printSnapshotTable(w, buildSnapshotRows(list.Items), allNamespaces)
	default:
		return fmt.Errorf("%w %q; use table|json|yaml", errUnsupportedFormat, outputFmt)
	}
}

// snapshotRow is the table-ready projection of a Snapshot object.
type snapshotRow struct {
	Namespace string
	Name      string
	Ready     string
	Children  int
	Age       string
}

// buildSnapshotRows projects unstructured Snapshot objects into table rows.
func buildSnapshotRows(items []unstructured.Unstructured) []snapshotRow {
	rows := make([]snapshotRow, 0, len(items))

	for i := range items {
		obj := &items[i]

		children, _, _ := unstructured.NestedSlice(obj.Object, "status", "childrenSnapshotRefs")

		rows = append(rows, snapshotRow{
			Namespace: obj.GetNamespace(),
			Name:      obj.GetName(),
			Ready:     readyStatus(obj),
			Children:  len(children),
			Age:       humanAge(obj.GetCreationTimestamp().Time),
		})
	}

	return rows
}

// readyStatus returns the status of the "Ready" condition ("True"/"False"/
// "Unknown") or "-" when the snapshot carries no such condition yet.
func readyStatus(obj *unstructured.Unstructured) string {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return notAvailable
	}

	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		condType, _, _ := unstructured.NestedString(m, "type")
		if condType != readyConditionType {
			continue
		}

		if status, _, _ := unstructured.NestedString(m, "status"); status != "" {
			return status
		}

		return notAvailable
	}

	return notAvailable
}

// printSnapshotTable writes rows as an aligned, kubectl-style table. With
// allNamespaces it prepends a NAMESPACE column.
func printSnapshotTable(w io.Writer, rows []snapshotRow, allNamespaces bool) error {
	if len(rows) == 0 {
		fmt.Fprintln(w, "No snapshots found.")
		return nil
	}

	tw := printers.GetNewTabWriter(w)

	if allNamespaces {
		fmt.Fprintln(tw, "NAMESPACE\tNAME\tREADY\tCHILDREN\tAGE")
	} else {
		fmt.Fprintln(tw, "NAME\tREADY\tCHILDREN\tAGE")
	}

	for _, r := range rows {
		if allNamespaces {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n",
				r.Namespace, r.Name, r.Ready, r.Children, r.Age)
		} else {
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n",
				r.Name, r.Ready, r.Children, r.Age)
		}
	}

	return tw.Flush()
}

// humanAge formats a creation timestamp as the compact age column kubectl uses
// (e.g. "5m", "2h", "3d"), delegating to apimachinery for consistency.
func humanAge(t time.Time) string {
	if t.IsZero() {
		return notAvailable
	}

	return duration.HumanDuration(time.Since(t))
}
