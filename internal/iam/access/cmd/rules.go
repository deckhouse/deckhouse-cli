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

package access

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/templates"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// ruleRow is a uniform view over a CAR or an AR for list/get rendering.
type ruleRow struct {
	Kind            string // ClusterAuthorizationRule | AuthorizationRule
	Name            string
	Namespace       string // empty for CAR
	AccessLevel     string
	ScopeType       string   // cluster | all-namespaces | namespace
	ScopeNamespaces []string // for namespace scope (the single namespace of the AR)
	AllowScale      bool
	PortForwarding  bool
	ManagedByD8     bool
	Subjects        []subjectRef
	CreationTime    time.Time
}

type subjectRef struct {
	Kind string // User | Group | ServiceAccount | ...
	Name string
}

func (r ruleRow) ref() string {
	if r.Namespace == "" {
		return r.Kind + "/" + r.Name
	}
	return r.Kind + "/" + r.Namespace + "/" + r.Name
}

// ------------------------------ cobra ------------------------------

var rulesLong = templates.LongDesc(`
Inspect the raw authorization rules (ClusterAuthorizationRule and
AuthorizationRule) that back "d8 iam access grant/revoke/explain".

Unlike "d8 iam access list users|groups" (which aggregates effective access
per subject), "d8 iam access rules list" shows the rules themselves — d8-managed
and manual alike — in a single view so you can see "what objects exist and
what they give".

Use "d8 iam access rules get REF" for a detailed human view of one rule, with
a reverse lookup from spec.subjects to local User/Group CRs.

© Flant JSC 2026`)

func newRulesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rules",
		Short: "Inspect raw ClusterAuthorizationRule / AuthorizationRule objects",
		Long:  rulesLong,
	}
	cmd.AddCommand(newRulesListCommand(), newRulesGetCommand())
	return cmd
}

// ------------------------------ list ------------------------------

var rulesListExample = templates.Examples(`
  # All rules in the cluster (both CARs and every AR across namespaces)
  d8 iam access rules list

  # Only ClusterAuthorizationRules
  d8 iam access rules list --cluster

  # Only AuthorizationRules from selected namespaces
  d8 iam access rules list -n dev -n stage

  # Only rules created by "d8 iam access grant"
  d8 iam access rules list --managed-only

  # Only rules NOT created by d8-cli
  d8 iam access rules list --manual-only

  # Machine-readable
  d8 iam access rules list -o json
  d8 iam access rules list -o yaml`)

func newRulesListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "list",
		Short:         "List ClusterAuthorizationRules and AuthorizationRules",
		Example:       rulesListExample,
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          runRulesList,
	}

	cmd.Flags().StringSliceP("namespace", "n", nil, "Restrict to AuthorizationRules in these namespaces (repeatable). Implicitly disables CARs unless --cluster is also set.")
	cmd.Flags().Bool("cluster", false, "Only ClusterAuthorizationRules")
	cmd.Flags().Bool("managed-only", false, "Only rules managed by d8-cli (label app.kubernetes.io/managed-by=d8-cli)")
	cmd.Flags().Bool("manual-only", false, "Only rules NOT managed by d8-cli")
	cmd.Flags().StringP("output", "o", "table", "Output format: table|json|yaml")

	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespacesFlag)
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("table", "json", "yaml"))

	return cmd
}

func runRulesList(cmd *cobra.Command, _ []string) error {
	namespaces, _ := cmd.Flags().GetStringSlice("namespace")
	clusterOnly, _ := cmd.Flags().GetBool("cluster")
	managedOnly, _ := cmd.Flags().GetBool("managed-only")
	manualOnly, _ := cmd.Flags().GetBool("manual-only")
	outputFmt, _ := cmd.Flags().GetString("output")

	if managedOnly && manualOnly {
		return fmt.Errorf("--managed-only and --manual-only are mutually exclusive")
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	// Decide which kinds to fetch.
	//   --cluster          -> only CARs
	//   -n given, no --cluster -> only ARs in those namespaces
	//   nothing            -> everything (CARs + ARs in all namespaces)
	//   --cluster AND -n   -> CARs + ARs in those namespaces (explicit union)
	includeCARs := clusterOnly || len(namespaces) == 0
	includeARs := !clusterOnly || len(namespaces) > 0
	nsFilter := namespaces

	rows, err := collectRuleRows(cmd.Context(), dyn, includeCARs, includeARs, nsFilter)
	if err != nil {
		return err
	}

	rows = filterByManagement(rows, managedOnly, manualOnly)
	sortRuleRows(rows)

	switch outputFmt {
	case "json":
		return printRuleRowsJSON(cmd.OutOrStdout(), rows)
	case "yaml":
		return printRuleRowsYAML(cmd.OutOrStdout(), rows)
	case "table", "":
		return printRuleRowsTable(cmd.OutOrStdout(), rows)
	default:
		return fmt.Errorf("unsupported output format %q; use table|json|yaml", outputFmt)
	}
}

// ------------------------------ get ------------------------------

var rulesGetExample = templates.Examples(`
  # Get a ClusterAuthorizationRule (full prefix or short)
  d8 iam access rules get ClusterAuthorizationRule/superadmins
  d8 iam access rules get CAR/superadmins

  # Get an AuthorizationRule (namespace is part of the reference)
  d8 iam access rules get AuthorizationRule/dev/editors
  d8 iam access rules get AR/dev/editors

  # Machine-readable
  d8 iam access rules get CAR/superadmins -o yaml
  d8 iam access rules get AR/dev/editors -o json`)

func newRulesGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "get REF",
		Short:             "Show a single CAR or AR with subjects, scope and reverse CR lookup",
		Example:           rulesGetExample,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeRuleRef,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              runRulesGet,
	}
	cmd.Flags().StringP("output", "o", "text", "Output format: text|json|yaml")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("text", "json", "yaml"))
	return cmd
}

func runRulesGet(cmd *cobra.Command, args []string) error {
	refKind, refNS, refName, err := parseRuleRef(args[0])
	if err != nil {
		return err
	}
	outputFmt, _ := cmd.Flags().GetString("output")

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	var obj *unstructured.Unstructured
	switch refKind {
	case "ClusterAuthorizationRule":
		obj, err = dyn.Resource(clusterAuthorizationRuleGVR).Get(cmd.Context(), refName, metav1.GetOptions{})
	case "AuthorizationRule":
		obj, err = dyn.Resource(authorizationRuleGVR).Namespace(refNS).Get(cmd.Context(), refName, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("getting %s: %w", args[0], err)
	}

	switch outputFmt {
	case "yaml", "json":
		return utilk8s.PrintObject(cmd.OutOrStdout(), obj, outputFmt)
	case "text", "":
		row := ruleRowFromObject(obj)
		reverse, err := reverseSubjectLookup(cmd.Context(), dyn, row.Subjects)
		if err != nil {
			// Reverse lookup is a best-effort nicety; degrade rather than fail.
			fmt.Fprintf(cmd.ErrOrStderr(), "Warning: reverse lookup of subjects failed: %v\n", err)
		}
		return printRuleRowText(cmd.OutOrStdout(), row, reverse)
	default:
		return fmt.Errorf("unsupported output format %q; use text|json|yaml", outputFmt)
	}
}

// ------------------------------ collection ------------------------------

func collectRuleRows(ctx context.Context, dyn dynamic.Interface, includeCARs, includeARs bool, nsFilter []string) ([]ruleRow, error) {
	var rows []ruleRow

	if includeCARs {
		list, err := dyn.Resource(clusterAuthorizationRuleGVR).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing ClusterAuthorizationRules: %w", err)
		}
		for i := range list.Items {
			rows = append(rows, ruleRowFromObject(&list.Items[i]))
		}
	}

	if includeARs {
		nsToList := []string{""}
		if len(nsFilter) > 0 {
			nsToList = nsFilter
		}
		for _, ns := range nsToList {
			list, err := dyn.Resource(authorizationRuleGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, fmt.Errorf("listing AuthorizationRules in %q: %w", ns, err)
			}
			for i := range list.Items {
				rows = append(rows, ruleRowFromObject(&list.Items[i]))
			}
		}
	}

	return rows, nil
}

func ruleRowFromObject(obj *unstructured.Unstructured) ruleRow {
	labels := obj.GetLabels()

	accessLevel, _, _ := unstructured.NestedString(obj.Object, "spec", "accessLevel")
	allowScale, _, _ := unstructured.NestedBool(obj.Object, "spec", "allowScale")
	portForwarding, _, _ := unstructured.NestedBool(obj.Object, "spec", "portForwarding")

	kind := obj.GetKind()
	ns := obj.GetNamespace()
	scopeType := ""
	var scopeNamespaces []string
	switch kind {
	case "ClusterAuthorizationRule":
		scopeType = "cluster"
		matchAny, found, _ := unstructured.NestedBool(obj.Object, "spec", "namespaceSelector", "matchAny")
		if found && matchAny {
			scopeType = "all-namespaces"
		}
	case "AuthorizationRule":
		scopeType = "namespace"
		scopeNamespaces = []string{ns}
	}

	var subjects []subjectRef
	raw, _, _ := unstructured.NestedSlice(obj.Object, "spec", "subjects")
	for _, s := range raw {
		m, ok := s.(map[string]any)
		if !ok {
			continue
		}
		subjects = append(subjects, subjectRef{
			Kind: fmt.Sprint(m["kind"]),
			Name: fmt.Sprint(m["name"]),
		})
	}

	return ruleRow{
		Kind:            kind,
		Name:            obj.GetName(),
		Namespace:       ns,
		AccessLevel:     accessLevel,
		ScopeType:       scopeType,
		ScopeNamespaces: scopeNamespaces,
		AllowScale:      allowScale,
		PortForwarding:  portForwarding,
		ManagedByD8:     labels[managedByLabel] == managedByValue,
		Subjects:        subjects,
		CreationTime:    obj.GetCreationTimestamp().Time,
	}
}

func filterByManagement(rows []ruleRow, managedOnly, manualOnly bool) []ruleRow {
	if !managedOnly && !manualOnly {
		return rows
	}
	out := rows[:0]
	for _, r := range rows {
		switch {
		case managedOnly && r.ManagedByD8:
			out = append(out, r)
		case manualOnly && !r.ManagedByD8:
			out = append(out, r)
		}
	}
	return out
}

func sortRuleRows(rows []ruleRow) {
	sort.Slice(rows, func(i, j int) bool {
		// Stable, deterministic order: Kind, Namespace, Name.
		if rows[i].Kind != rows[j].Kind {
			// ClusterAuthorizationRule first — cluster-wide rules read before
			// namespaced ones, matching operator mental model.
			return rows[i].Kind < rows[j].Kind
		}
		if rows[i].Namespace != rows[j].Namespace {
			return rows[i].Namespace < rows[j].Namespace
		}
		return rows[i].Name < rows[j].Name
	})
}

// ------------------------------ rendering: table ------------------------------

// shortKind maps full CRD names to compact column values for the table view.
func shortKind(kind string) string {
	switch kind {
	case "ClusterAuthorizationRule":
		return "CAR"
	case "AuthorizationRule":
		return "AR"
	default:
		return kind
	}
}

func managedByColumn(r ruleRow) string {
	if r.ManagedByD8 {
		return "d8-cli"
	}
	return "manual"
}

func capsColumn(r ruleRow) string {
	var caps []string
	if r.AllowScale {
		caps = append(caps, "scale")
	}
	if r.PortForwarding {
		caps = append(caps, "pfwd")
	}
	if len(caps) == 0 {
		return "-"
	}
	return strings.Join(caps, ",")
}

func printRuleRowsTable(w io.Writer, rows []ruleRow) error {
	if len(rows) == 0 {
		fmt.Fprintln(w, "No rules match.")
		return nil
	}

	tw := printers.GetNewTabWriter(w)
	fmt.Fprintln(tw, "KIND\tNAMESPACE\tNAME\tLEVEL\tSCOPE\tSUBJECTS\tMANAGED\tCAPS\tAGE")

	for _, r := range rows {
		ns := r.Namespace
		if ns == "" {
			ns = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
			shortKind(r.Kind),
			ns,
			truncate(r.Name, 60),
			r.AccessLevel,
			r.ScopeType,
			len(r.Subjects),
			managedByColumn(r),
			capsColumn(r),
			humanAge(r.CreationTime),
		)
	}
	return tw.Flush()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 1 {
		return s[:max]
	}
	return s[:max-1] + "…"
}

func humanAge(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// ------------------------------ rendering: text (get) ------------------------------

func printRuleRowText(w io.Writer, r ruleRow, reverse map[string]string) error {
	fmt.Fprintf(w, "=== %s ===\n", r.ref())
	fmt.Fprintf(w, "  Kind:         %s\n", r.Kind)
	fmt.Fprintf(w, "  Name:         %s\n", r.Name)
	if r.Namespace != "" {
		fmt.Fprintf(w, "  Namespace:    %s\n", r.Namespace)
	}
	fmt.Fprintf(w, "  Access level: %s\n", firstNonEmpty(r.AccessLevel, "<unset>"))
	fmt.Fprintf(w, "  Scope:        %s\n", r.ScopeType)
	fmt.Fprintf(w, "  Allow scale:  %v\n", r.AllowScale)
	fmt.Fprintf(w, "  Port forward: %v\n", r.PortForwarding)
	fmt.Fprintf(w, "  Managed by:   %s\n", managedByColumn(r))
	if !r.CreationTime.IsZero() {
		fmt.Fprintf(w, "  Age:          %s (%s)\n", humanAge(r.CreationTime), r.CreationTime.UTC().Format(time.RFC3339))
	}

	fmt.Fprintf(w, "\n=== Subjects (%d) ===\n", len(r.Subjects))
	if len(r.Subjects) == 0 {
		fmt.Fprintln(w, "  <none>")
	} else {
		for _, s := range r.Subjects {
			key := s.Kind + "/" + s.Name
			local := reverse[key]
			switch local {
			case "":
				fmt.Fprintf(w, "  - %s: %s\n", s.Kind, s.Name)
			case s.Name:
				// Group: the principal IS the CR name — no extra info to show.
				fmt.Fprintf(w, "  - %s: %s\n", s.Kind, s.Name)
			default:
				// User: principal (email) differs from CR name. Surface both.
				fmt.Fprintf(w, "  - %s: %s (local %s CR: %s)\n", s.Kind, s.Name, s.Kind, local)
			}
		}
	}

	if r.AccessLevel == "SuperAdmin" && (!r.AllowScale || !r.PortForwarding) {
		fmt.Fprintln(w, "\n=== Notes ===")
		fmt.Fprintln(w, "  - accessLevel=SuperAdmin binds user-authz:super-admin ClusterRole,")
		fmt.Fprintln(w, "    which grants apiGroups/resources/verbs=* and nonResourceURLs=*.")
		fmt.Fprintln(w, "    This implicitly covers pods/portforward and */scale regardless of")
		fmt.Fprintln(w, "    the allowScale / portForwarding fields on this CAR.")
	}

	return nil
}

func firstNonEmpty(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// ------------------------------ rendering: json/yaml ------------------------------

type ruleJSON struct {
	Kind           string        `json:"kind"`
	Name           string        `json:"name"`
	Namespace      string        `json:"namespace,omitempty"`
	AccessLevel    string        `json:"accessLevel"`
	Scope          string        `json:"scope"`
	AllowScale     bool          `json:"allowScale"`
	PortForwarding bool          `json:"portForwarding"`
	ManagedByD8    bool          `json:"managedByD8Cli"`
	Subjects       []subjectJSON `json:"subjects"`
	CreationTime   time.Time     `json:"creationTimestamp,omitempty"`
}

type subjectJSON struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

func ruleRowToJSON(r ruleRow) ruleJSON {
	out := ruleJSON{
		Kind:           r.Kind,
		Name:           r.Name,
		Namespace:      r.Namespace,
		AccessLevel:    r.AccessLevel,
		Scope:          r.ScopeType,
		AllowScale:     r.AllowScale,
		PortForwarding: r.PortForwarding,
		ManagedByD8:    r.ManagedByD8,
		CreationTime:   r.CreationTime,
	}
	for _, s := range r.Subjects {
		out.Subjects = append(out.Subjects, subjectJSON(s))
	}
	if out.Subjects == nil {
		out.Subjects = []subjectJSON{}
	}
	return out
}

func printRuleRowsJSON(w io.Writer, rows []ruleRow) error {
	items := make([]ruleJSON, 0, len(rows))
	for _, r := range rows {
		items = append(items, ruleRowToJSON(r))
	}
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(w, string(data))
	return nil
}

func printRuleRowsYAML(w io.Writer, rows []ruleRow) error {
	// YAML output is intentionally the same shape as JSON for symmetry with
	// other list commands in this package (we do not emit full Kubernetes
	// manifests here — use "rules get -o yaml" for that).
	items := make([]ruleJSON, 0, len(rows))
	for _, r := range rows {
		items = append(items, ruleRowToJSON(r))
	}
	data, err := json.Marshal(items)
	if err != nil {
		return err
	}
	// sigs.k8s.io/yaml keeps formatting consistent with utilk8s.PrintObject.
	yamlBytes, err := sigsyaml.JSONToYAML(data)
	if err != nil {
		return err
	}
	fmt.Fprint(w, string(yamlBytes))
	return nil
}

// ------------------------------ refs ------------------------------

// parseRuleRef accepts:
//
//	ClusterAuthorizationRule/NAME      (long)
//	CAR/NAME                           (short)
//	AuthorizationRule/NS/NAME          (long)
//	AR/NS/NAME                         (short)
//
// Returns (kind, namespace, name, err). namespace is "" for CARs.
func parseRuleRef(ref string) (string, string, string, error) {
	parts := strings.Split(ref, "/")
	if len(parts) < 2 {
		return "", "", "", fmt.Errorf("invalid rule reference %q; expected ClusterAuthorizationRule/NAME or AuthorizationRule/NS/NAME (short forms CAR/... and AR/... also accepted)", ref)
	}

	switch parts[0] {
	case "ClusterAuthorizationRule", "CAR", "car":
		if len(parts) != 2 {
			return "", "", "", fmt.Errorf("ClusterAuthorizationRule reference must be of the form ClusterAuthorizationRule/NAME (got %q)", ref)
		}
		return "ClusterAuthorizationRule", "", parts[1], nil
	case "AuthorizationRule", "AR", "ar":
		if len(parts) != 3 {
			return "", "", "", fmt.Errorf("AuthorizationRule reference must be of the form AuthorizationRule/NAMESPACE/NAME (got %q)", ref)
		}
		return "AuthorizationRule", parts[1], parts[2], nil
	default:
		return "", "", "", fmt.Errorf("unknown rule kind %q; use ClusterAuthorizationRule, AuthorizationRule, CAR or AR", parts[0])
	}
}

// ------------------------------ reverse lookup ------------------------------

// reverseSubjectLookup returns a map "Kind/Name" -> local-CR-name for subjects
// that can be cross-referenced with local User/Group CRs. For Users we look up
// by spec.email because that is what ends up in subjects[].name. For Groups
// the subject name IS the CR name, so the map simply echoes it (useful for
// the "rendered with no extra info" branch in the text output).
func reverseSubjectLookup(ctx context.Context, dyn dynamic.Interface, subjects []subjectRef) (map[string]string, error) {
	result := make(map[string]string)
	needUsers := false
	needGroups := false
	for _, s := range subjects {
		switch s.Kind {
		case "User":
			needUsers = true
		case "Group":
			needGroups = true
		}
	}

	if needUsers {
		userList, err := dyn.Resource(userGVR).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing Users: %w", err)
		}
		emailToCR := make(map[string]string, len(userList.Items))
		for i := range userList.Items {
			u := &userList.Items[i]
			email, _, _ := unstructured.NestedString(u.Object, "spec", "email")
			if email != "" {
				emailToCR[email] = u.GetName()
			}
		}
		for _, s := range subjects {
			if s.Kind != "User" {
				continue
			}
			if cr, ok := emailToCR[s.Name]; ok {
				result["User/"+s.Name] = cr
			}
		}
	}

	if needGroups {
		groupList, err := dyn.Resource(groupGVR).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing Groups: %w", err)
		}
		present := make(map[string]bool, len(groupList.Items))
		for i := range groupList.Items {
			present[groupList.Items[i].GetName()] = true
		}
		for _, s := range subjects {
			if s.Kind != "Group" {
				continue
			}
			if present[s.Name] {
				result["Group/"+s.Name] = s.Name
			}
		}
	}

	return result, nil
}
