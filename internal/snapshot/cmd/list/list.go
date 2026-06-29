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

// Package list implements the `d8 snapshot list` command.
package list

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/dynamic"
	sigsyaml "sigs.k8s.io/yaml"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const (
	cmdUse = "list"

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

// snapshotGVR is the dynamic resource for storage.deckhouse.io Snapshots.
var snapshotGVR = schema.GroupVersionResource{
	Group:    snapshotapi.StorageGroup,
	Version:  snapshotapi.Version,
	Resource: "snapshots",
}

// NewCommand builds the `d8 snapshot list` cobra command.
func NewCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           cmdUse + " [DIR]",
		Aliases:       []string{"ls"},
		Short:         "List Snapshot resources (cluster or a local download directory)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.MaximumNArgs(1),
		Example: `  # List snapshots in the kubeconfig context namespace
  d8 snapshot list

  # List snapshots in a specific namespace
  d8 snapshot list -n my-namespace

  # List snapshots across all namespaces
  d8 snapshot list -A

  # List snapshot nodes from a local download directory (offline, no cluster access)
  d8 snapshot list ./out

  # Machine-readable output
  d8 snapshot list -o json
  d8 snapshot list ./out -o yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(log, cmd, args)
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
func Run(log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	outputFmt, err := cmd.Flags().GetString(flagOutput)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagOutput, err)
	}

	// Local mode: a positional directory argument lists snapshot nodes from a
	// previously downloaded archive tree, without contacting the cluster.
	if len(args) == 1 {
		return runLocal(cmd, args[0], outputFmt)
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
	Namespace       string
	Name            string
	Ready           string
	SnapshotContent string
	Children        int
	Age             string
}

// buildSnapshotRows projects unstructured Snapshot objects into table rows.
func buildSnapshotRows(items []unstructured.Unstructured) []snapshotRow {
	rows := make([]snapshotRow, 0, len(items))

	for i := range items {
		obj := &items[i]

		content, _, _ := unstructured.NestedString(obj.Object, "status", "boundSnapshotContentName")
		if content == "" {
			content = notAvailable
		}

		children, _, _ := unstructured.NestedSlice(obj.Object, "status", "childrenSnapshotRefs")

		rows = append(rows, snapshotRow{
			Namespace:       obj.GetNamespace(),
			Name:            obj.GetName(),
			Ready:           readyStatus(obj),
			SnapshotContent: content,
			Children:        len(children),
			Age:             humanAge(obj.GetCreationTimestamp().Time),
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
		fmt.Fprintln(tw, "NAMESPACE\tNAME\tREADY\tSNAPSHOTCONTENT\tCHILDREN\tAGE")
	} else {
		fmt.Fprintln(tw, "NAME\tREADY\tSNAPSHOTCONTENT\tCHILDREN\tAGE")
	}

	for _, r := range rows {
		if allNamespaces {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
				r.Namespace, r.Name, r.Ready, r.SnapshotContent, r.Children, r.Age)
		} else {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n",
				r.Name, r.Ready, r.SnapshotContent, r.Children, r.Age)
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

// localSnapshotRow is one snapshot node discovered in a local download tree.
// JSON tags drive the -o json|yaml output.
type localSnapshotRow struct {
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Children  int    `json:"children"`
	Volumes   int    `json:"volumes"`
	Age       string `json:"age"`
	// Path is the node directory relative to the scanned root (`.` for the root).
	Path string `json:"path"`
}

// runLocal lists snapshot nodes found under a local download directory. Cluster
// scope flags are rejected because the cluster is not consulted in this mode.
func runLocal(cmd *cobra.Command, dir, outputFmt string) error {
	if cmd.Flags().Changed(flagAllNamespaces) || cmd.Flags().Changed(flagNamespace) {
		return fmt.Errorf("--%s/--%s are not applicable with a local directory argument", flagAllNamespaces, flagNamespace)
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolving path %q: %w", dir, err)
	}

	rows, err := discoverLocalSnapshots(absDir)
	if err != nil {
		return err
	}

	return renderLocal(cmd.OutOrStdout(), rows, outputFmt)
}

// discoverLocalSnapshots walks root and returns one row per snapshot node, i.e.
// per directory containing a snapshot.yaml. Walking depth-first in lexical order
// yields a natural parent-before-child ordering of the tree.
func discoverLocalSnapshots(root string) ([]localSnapshotRow, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("accessing %q: %w", root, err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("%q is not a directory", root)
	}

	rows := make([]localSnapshotRow, 0)

	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if !d.IsDir() {
			return nil
		}

		syPath := filepath.Join(path, archive.SnapshotYAMLName)

		if _, statErr := os.Stat(syPath); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}

			return fmt.Errorf("stat %s: %w", syPath, statErr)
		}

		row, rowErr := buildLocalRow(root, path)
		if rowErr != nil {
			return rowErr
		}

		rows = append(rows, row)

		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("scanning %q: %w", root, walkErr)
	}

	return rows, nil
}

// buildLocalRow reads the snapshot.yaml at nodeDir and projects it into a row.
func buildLocalRow(root, nodeDir string) (localSnapshotRow, error) {
	sy, err := archive.ReadSnapshotYAML(nodeDir)
	if err != nil {
		return localSnapshotRow{}, fmt.Errorf("read %s: %w", filepath.Join(nodeDir, archive.SnapshotYAMLName), err)
	}

	rel, err := filepath.Rel(root, nodeDir)
	if err != nil {
		rel = nodeDir
	}

	children, err := countChildNodes(nodeDir)
	if err != nil {
		return localSnapshotRow{}, err
	}

	// Namespace is stored raw (empty for cluster-scoped) so -o json/yaml stays
	// faithful; the table renderer substitutes the "-" placeholder.
	return localSnapshotRow{
		Name:      sy.Name,
		Kind:      sy.Kind,
		Namespace: sy.Namespace,
		Children:  children,
		Volumes:   len(sy.Volumes),
		Age:       localNodeAge(nodeDir),
		Path:      rel,
	}, nil
}

// countChildNodes counts immediate child node directories (those holding a
// snapshot.yaml) under nodeDir/snapshots/. A missing snapshots/ directory means
// zero children; any other I/O error is propagated so the listing never
// silently under-reports.
func countChildNodes(nodeDir string) (int, error) {
	snapshotsDir := filepath.Join(nodeDir, archive.SnapshotsDirName)

	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("read %s: %w", snapshotsDir, err)
	}

	count := 0

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		childSY := filepath.Join(snapshotsDir, e.Name(), archive.SnapshotYAMLName)

		switch _, statErr := os.Stat(childSY); {
		case statErr == nil:
			count++
		case errors.Is(statErr, os.ErrNotExist):
			// Not a snapshot node directory; ignore.
		default:
			return 0, fmt.Errorf("stat %s: %w", childSY, statErr)
		}
	}

	return count, nil
}

// localNodeAge derives the AGE column from the snapshot.yaml modification time.
func localNodeAge(nodeDir string) string {
	info, err := os.Stat(filepath.Join(nodeDir, archive.SnapshotYAMLName))
	if err != nil {
		return notAvailable
	}

	return humanAge(info.ModTime())
}

// renderLocal dispatches local rows to the requested output format.
func renderLocal(w io.Writer, rows []localSnapshotRow, outputFmt string) error {
	switch outputFmt {
	case "json", "yaml":
		return printStructured(w, rows, outputFmt)
	case "table", "":
		return printLocalTable(w, rows)
	default:
		return fmt.Errorf("%w %q; use table|json|yaml", errUnsupportedFormat, outputFmt)
	}
}

// printLocalTable writes local snapshot rows as an aligned table.
func printLocalTable(w io.Writer, rows []localSnapshotRow) error {
	if len(rows) == 0 {
		fmt.Fprintln(w, "No snapshots found.")
		return nil
	}

	tw := printers.GetNewTabWriter(w)
	fmt.Fprintln(tw, "NAME\tKIND\tNAMESPACE\tCHILDREN\tVOLUMES\tAGE\tPATH")

	for _, r := range rows {
		namespace := r.Namespace
		if namespace == "" {
			namespace = notAvailable
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			r.Name, r.Kind, namespace, r.Children, r.Volumes, r.Age, r.Path)
	}

	return tw.Flush()
}

// printStructured renders v as JSON or YAML. YAML goes through json.Marshal +
// sigsyaml.JSONToYAML so json struct tags drive field names.
func printStructured(w io.Writer, v any, format string) error {
	switch format {
	case "json":
		data, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Errorf("marshalling JSON: %w", err)
		}

		fmt.Fprintln(w, string(data))

		return nil
	case "yaml":
		jsonData, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("marshalling JSON for YAML conversion: %w", err)
		}

		yamlData, err := sigsyaml.JSONToYAML(jsonData)
		if err != nil {
			return fmt.Errorf("converting JSON to YAML: %w", err)
		}

		fmt.Fprint(w, string(yamlData))

		return nil
	default:
		return fmt.Errorf("%w %q", errUnsupportedFormat, format)
	}
}
