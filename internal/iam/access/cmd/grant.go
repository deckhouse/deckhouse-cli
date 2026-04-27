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
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var grantLong = templates.LongDesc(`
Grant access to a user or group using the current authorization model.

For users, the subject is resolved by reading User.spec.email from the cluster,
because the current authz model requires email as the subject name when user-authn
is used.

Exactly one scope flag is required (they are mutually exclusive):

  -n/--namespace   Creates an AuthorizationRule per namespace (namespaced scope).
                   Only User, PrivilegedUser, Editor, Admin levels are valid.

  --cluster        Creates a ClusterAuthorizationRule. The grant applies to all
                   namespaces EXCEPT d8-* and kube-system (system namespaces).
                   All access levels are valid, including ClusterEditor/ClusterAdmin/SuperAdmin.

  --all-namespaces Creates a ClusterAuthorizationRule with namespaceSelector.matchAny.
                   The grant covers ALL namespaces including system namespaces.
                   Use with caution — grants access to d8-system, kube-system, etc.

Modifier flags --allow-scale and --port-forwarding add additional capabilities
to the grant and can be combined with any scope.

ADVANCED MODE (--to):

  Adds a subject to an existing authorization rule instead of creating a new
  d8-managed object. Useful when you want to extend a rule maintained
  manually or shared across teams.

      d8 iam access grant --to ClusterAuthorizationRule/RULE  user PRINCIPAL
      d8 iam access grant --to AuthorizationRule/NS/RULE      group GROUPNAME

  PRINCIPAL is written literally into spec.subjects[].name — for users pass
  the exact principal expected by the authz module (typically the email),
  not the User CR name. No User CR resolution is performed in this mode.

  --to is strictly "add a subject". It never modifies accessLevel, scope,
  allowScale or portForwarding of the target rule. Those fields apply to
  ALL subjects of the rule, so changing them from CLI would silently affect
  other principals — that is by design disallowed. Passing any of these
  flags together with --to is an error.

  Because RBAC is additive, the normal way to extend a subject's capabilities
  is a separate d8-managed grant next to the existing rule:

      d8 iam access grant user alice --access-level Admin -n dev --port-forwarding

  If you really need to change a shared rule's flags for everyone on it, edit
  the rule directly (kubectl edit), since that is a policy change, not a
  per-subject grant.

© Flant JSC 2026`)

var grantExample = templates.Examples(`
  # Grant Admin in specific namespaces
  d8 iam access grant user anton --access-level Admin -n dev -n stage

  # Grant ClusterAdmin cluster-wide (system namespaces excluded)
  d8 iam access grant user anton --access-level ClusterAdmin --cluster

  # Grant ClusterAdmin to ALL namespaces including d8-system, kube-system
  d8 iam access grant user anton --access-level ClusterAdmin --all-namespaces

  # Grant Editor to a group with port-forwarding enabled
  d8 iam access grant group admins --access-level Editor -n dev --port-forwarding

  # Dry-run to preview the manifest before applying
  d8 iam access grant user anton --access-level Admin -n dev --dry-run -o yaml

  # Advanced mode: add a user to an existing ClusterAuthorizationRule (literal principal)
  d8 iam access grant --to ClusterAuthorizationRule/superadmins user new@example.com

  # Advanced mode: add a group to an existing namespaced AuthorizationRule
  d8 iam access grant --to AuthorizationRule/dev/shared-editors group devs`)

func newGrantCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "grant (user|group) NAME [--access-level LEVEL scope | --to SOURCE]",
		Short:             "Grant access to a user or group (current authz model)",
		Long:              grantLong,
		Example:           grantExample,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeSubjectAndName,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              runGrant,
	}

	cmd.Flags().String("access-level", "", "Access level: User, PrivilegedUser, Editor, Admin, ClusterEditor, ClusterAdmin, SuperAdmin")
	cmd.Flags().StringSliceP("namespace", "n", nil, "Target namespace(s) for namespaced grants (repeatable)")
	cmd.Flags().Bool("cluster", false, "Cluster scope: all namespaces EXCEPT system (d8-*, kube-system)")
	cmd.Flags().Bool("all-namespaces", false, "Cluster scope: ALL namespaces INCLUDING system (d8-*, kube-system)")
	cmd.Flags().Bool("port-forwarding", false, "Allow port-forwarding")
	cmd.Flags().Bool("allow-scale", false, "Allow scaling workloads")
	cmd.Flags().Bool("dry-run", false, "Print the resource(s) that would be created without applying")
	cmd.Flags().StringP("output", "o", "name", "Output format: name|yaml|json")
	cmd.Flags().String("to", "", "Existing rule to add the subject to: AuthorizationRule/<ns>/<name> or ClusterAuthorizationRule/<name>")

	_ = cmd.RegisterFlagCompletionFunc("access-level", completeAccessLevels)
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespacesFlag)
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("name", "yaml", "json"))
	_ = cmd.RegisterFlagCompletionFunc("to", completeRuleRef)

	return cmd
}

func runGrant(cmd *cobra.Command, args []string) error {
	toFlag, _ := cmd.Flags().GetString("to")
	if toFlag != "" {
		return runSourceBasedGrant(cmd, args)
	}

	subjectKindStr := args[0]
	subjectName := args[1]

	accessLevel, _ := cmd.Flags().GetString("access-level")
	namespaces, _ := cmd.Flags().GetStringSlice("namespace")
	clusterFlag, _ := cmd.Flags().GetBool("cluster")
	allNSFlag, _ := cmd.Flags().GetBool("all-namespaces")
	portForwarding, _ := cmd.Flags().GetBool("port-forwarding")
	allowScale, _ := cmd.Flags().GetBool("allow-scale")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	outputFmt, _ := cmd.Flags().GetString("output")

	if accessLevel == "" {
		return fmt.Errorf("--access-level is required (or use --to to add a subject to an existing rule)")
	}

	subjectKind, err := parseSubjectKind(subjectKindStr)
	if err != nil {
		return err
	}

	scopeType, err := parseScopeFlags(namespaces, clusterFlag, allNSFlag)
	if err != nil {
		return err
	}

	isNamespaced := scopeType == "namespace"
	if err := validateAccessLevel(accessLevel, isNamespaced); err != nil {
		return err
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	var subjectPrincipal string
	switch subjectKind {
	case "User":
		subjectPrincipal, err = resolveUserEmail(cmd.Context(), dyn, subjectName)
		if err != nil {
			return err
		}
	case "Group":
		subjectPrincipal = subjectName
		if _, err := dyn.Resource(groupGVR).Get(cmd.Context(), subjectName, metav1.GetOptions{}); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Warning: local Group CR %q not found. Grant may target an external provider group.\n", subjectName)
		}
	}

	switch scopeType {
	case "namespace":
		return grantNamespaced(cmd, dyn, subjectKind, subjectName, subjectPrincipal, accessLevel, namespaces, allowScale, portForwarding, dryRun, outputFmt)
	case "cluster", "all-namespaces":
		return grantCluster(cmd, dyn, subjectKind, subjectName, subjectPrincipal, accessLevel, scopeType, allowScale, portForwarding, dryRun, outputFmt)
	}
	return nil
}

func grantNamespaced(cmd *cobra.Command, dyn dynamic.Interface,
	subjectKind, subjectRef, subjectPrincipal, accessLevel string,
	namespaces []string, allowScale, portForwarding, dryRun bool, outputFmt string) error {
	var errs *multierror.Error
	for _, ns := range namespaces {
		spec := &canonicalGrantSpec{
			Model:            "current",
			SubjectKind:      subjectKind,
			SubjectRef:       subjectRef,
			SubjectPrincipal: subjectPrincipal,
			AccessLevel:      accessLevel,
			ScopeType:        "namespace",
			Namespaces:       []string{ns},
			AllowScale:       allowScale,
			PortForwarding:   portForwarding,
		}

		obj, err := buildAuthorizationRule(spec, ns)
		if err != nil {
			return err
		}

		if dryRun {
			if err := utilk8s.PrintObject(cmd.OutOrStdout(), obj, outputFmt); err != nil {
				return err
			}
			continue
		}

		result, err := createOrUpdateNamespacedGrant(cmd, dyn, obj, ns)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to create grant in namespace %q: %v\n", ns, err)
			errs = multierror.Append(errs, fmt.Errorf("namespace %s: %w", ns, err))
			continue
		}
		if err := utilk8s.PrintObject(cmd.OutOrStdout(), result, outputFmt); err != nil {
			return err
		}
	}

	return errs.ErrorOrNil()
}

func grantCluster(cmd *cobra.Command, dyn dynamic.Interface,
	subjectKind, subjectRef, subjectPrincipal, accessLevel, scopeType string,
	allowScale, portForwarding, dryRun bool, outputFmt string) error {
	spec := &canonicalGrantSpec{
		Model:            "current",
		SubjectKind:      subjectKind,
		SubjectRef:       subjectRef,
		SubjectPrincipal: subjectPrincipal,
		AccessLevel:      accessLevel,
		ScopeType:        scopeType,
		AllowScale:       allowScale,
		PortForwarding:   portForwarding,
	}

	obj, err := buildClusterAuthorizationRule(spec)
	if err != nil {
		return err
	}

	if dryRun {
		return utilk8s.PrintObject(cmd.OutOrStdout(), obj, outputFmt)
	}

	result, err := createOrUpdateClusterGrant(cmd, dyn, obj)
	if err != nil {
		return err
	}
	return utilk8s.PrintObject(cmd.OutOrStdout(), result, outputFmt)
}

func buildAuthorizationRule(spec *canonicalGrantSpec, ns string) (*unstructured.Unstructured, error) {
	name, err := generateGrantName(spec)
	if err != nil {
		return nil, err
	}
	labels := grantLabels(spec)
	annotations, err := grantAnnotations(spec)
	if err != nil {
		return nil, err
	}

	ruleSpec := map[string]any{
		"accessLevel":    spec.AccessLevel,
		"allowScale":     spec.AllowScale,
		"portForwarding": spec.PortForwarding,
		"subjects": []any{
			map[string]any{
				"kind": spec.SubjectKind,
				"name": spec.SubjectPrincipal,
			},
		},
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1alpha1",
			"kind":       "AuthorizationRule",
			"metadata": map[string]any{
				"name":        name,
				"namespace":   ns,
				"labels":      toAnyMap(labels),
				"annotations": toAnyMap(annotations),
			},
			"spec": ruleSpec,
		},
	}, nil
}

func buildClusterAuthorizationRule(spec *canonicalGrantSpec) (*unstructured.Unstructured, error) {
	name, err := generateGrantName(spec)
	if err != nil {
		return nil, err
	}
	labels := grantLabels(spec)
	annotations, err := grantAnnotations(spec)
	if err != nil {
		return nil, err
	}

	ruleSpec := map[string]any{
		"accessLevel":    spec.AccessLevel,
		"allowScale":     spec.AllowScale,
		"portForwarding": spec.PortForwarding,
		"subjects": []any{
			map[string]any{
				"kind": spec.SubjectKind,
				"name": spec.SubjectPrincipal,
			},
		},
	}

	if spec.ScopeType == "all-namespaces" {
		ruleSpec["namespaceSelector"] = map[string]any{
			"matchAny": true,
		}
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1",
			"kind":       "ClusterAuthorizationRule",
			"metadata": map[string]any{
				"name":        name,
				"labels":      toAnyMap(labels),
				"annotations": toAnyMap(annotations),
			},
			"spec": ruleSpec,
		},
	}, nil
}

func createOrUpdateNamespacedGrant(cmd *cobra.Command, dyn dynamic.Interface, obj *unstructured.Unstructured, ns string) (*unstructured.Unstructured, error) {
	client := dyn.Resource(authorizationRuleGVR).Namespace(ns)
	name := obj.GetName()

	existing, err := client.Get(cmd.Context(), name, metav1.GetOptions{})
	if err == nil {
		// Check if it matches
		existingAnnot := existing.GetAnnotations()
		newAnnot := obj.GetAnnotations()
		if existingAnnot["deckhouse.io/access-canonical-spec"] == newAnnot["deckhouse.io/access-canonical-spec"] {
			cmd.PrintErrf("AuthorizationRule/%s/%s unchanged\n", ns, name)
			return existing, nil
		}
		obj.SetResourceVersion(existing.GetResourceVersion())
		return client.Update(cmd.Context(), obj, metav1.UpdateOptions{})
	}

	return client.Create(cmd.Context(), obj, metav1.CreateOptions{})
}

func createOrUpdateClusterGrant(cmd *cobra.Command, dyn dynamic.Interface, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	client := dyn.Resource(clusterAuthorizationRuleGVR)
	name := obj.GetName()

	existing, err := client.Get(cmd.Context(), name, metav1.GetOptions{})
	if err == nil {
		existingAnnot := existing.GetAnnotations()
		newAnnot := obj.GetAnnotations()
		if existingAnnot["deckhouse.io/access-canonical-spec"] == newAnnot["deckhouse.io/access-canonical-spec"] {
			cmd.PrintErrf("ClusterAuthorizationRule/%s unchanged\n", name)
			return existing, nil
		}
		obj.SetResourceVersion(existing.GetResourceVersion())
		return client.Update(cmd.Context(), obj, metav1.UpdateOptions{})
	}

	return client.Create(cmd.Context(), obj, metav1.CreateOptions{})
}

func parseSubjectKind(s string) (string, error) {
	switch strings.ToLower(s) {
	case "user":
		return "User", nil
	case "group":
		return "Group", nil
	default:
		return "", fmt.Errorf("invalid subject kind %q: must be user or group", s)
	}
}

func parseScopeFlags(namespaces []string, cluster, allNamespaces bool) (string, error) {
	count := 0
	if len(namespaces) > 0 {
		count++
	}
	if cluster {
		count++
	}
	if allNamespaces {
		count++
	}
	if count == 0 {
		return "", fmt.Errorf("one of -n/--namespace, --cluster, or --all-namespaces must be specified")
	}
	if count > 1 {
		return "", fmt.Errorf("-n/--namespace, --cluster, and --all-namespaces are mutually exclusive")
	}

	if len(namespaces) > 0 {
		return "namespace", nil
	}
	if cluster {
		return "cluster", nil
	}
	return "all-namespaces", nil
}

func toAnyMap(m map[string]string) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// rejectFlagsInToMode fails fast if any flag incompatible with "--to add-subject"
// mode is provided. Each flag gets a specific hint so the user knows which tool
// to reach for instead.
func rejectFlagsInToMode(cmd *cobra.Command) error {
	ruleSpecHint := "this field applies to ALL subjects of the rule; change it with 'kubectl edit' on the rule, not via --to"
	perSubjectHint := "for a per-subject capability on top of the existing rule, create a separate grant without --to: 'd8 iam access grant <subject> --access-level ... [scope] --port-forwarding --allow-scale'"

	hints := map[string]string{
		"access-level":   ruleSpecHint,
		"namespace":      ruleSpecHint,
		"cluster":        ruleSpecHint,
		"all-namespaces": ruleSpecHint,
		"port-forwarding": perSubjectHint +
			" — note: setting it on a shared rule would affect every subject already on it",
		"allow-scale": perSubjectHint +
			" — note: setting it on a shared rule would affect every subject already on it",
		"dry-run": "--dry-run is not supported with --to (subject add is applied via Update, not generated manifest)",
	}

	var offenders []string
	for f := range hints {
		if cmd.Flags().Changed(f) {
			offenders = append(offenders, f)
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	sort.Strings(offenders)

	var b strings.Builder
	fmt.Fprintf(&b, "the following flag(s) are not allowed with --to: --%s\n", strings.Join(offenders, ", --"))
	for _, f := range offenders {
		fmt.Fprintf(&b, "  --%s: %s\n", f, hints[f])
	}
	return errors.New(b.String())
}

func runSourceBasedGrant(cmd *cobra.Command, args []string) error {
	toFlag, _ := cmd.Flags().GetString("to")

	// Hard-fail on any flag that would imply we are redefining the rule.
	// port-forwarding and allow-scale in particular apply to ALL subjects of
	// the target rule, so silently "applying" them via --to would silently
	// mutate other principals' capabilities. That's unsafe and must be
	// explicit — force the user to pick the right tool:
	//   - per-subject capability  -> separate d8-managed grant (RBAC is additive)
	//   - rule-wide policy change -> kubectl edit on the rule itself
	if err := rejectFlagsInToMode(cmd); err != nil {
		return err
	}

	if len(args) != 2 {
		return fmt.Errorf("source-based grant requires: grant --to <source> (user|group) <principal>")
	}
	subjectKind, err := parseSubjectKind(args[0])
	if err != nil {
		return err
	}
	// Principal is used literally — it must exactly match the value that will
	// land in spec.subjects[].name. No User CR lookup here, for symmetry with
	// `revoke --from` and because shared rules may already carry a specific
	// principal format (email, LDAP DN, etc).
	subjectPrincipal := args[1]

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	refKind, refNS, refName, err := parseRuleRef(toFlag)
	if err != nil {
		return fmt.Errorf("invalid --to: %w", err)
	}
	switch refKind {
	case "ClusterAuthorizationRule":
		return addSubjectToClusterRule(cmd, dyn, refName, subjectKind, subjectPrincipal)
	case "AuthorizationRule":
		return addSubjectToNamespacedRule(cmd, dyn, refNS, refName, subjectKind, subjectPrincipal)
	default:
		return fmt.Errorf("unsupported --to kind %q", refKind)
	}
}

func addSubjectToClusterRule(cmd *cobra.Command, dyn dynamic.Interface, ruleName, subjectKind, subjectPrincipal string) error {
	client := dyn.Resource(clusterAuthorizationRuleGVR)
	obj, err := client.Get(cmd.Context(), ruleName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting ClusterAuthorizationRule %q: %w", ruleName, err)
	}

	newSubjects, added := addSubject(obj, subjectKind, subjectPrincipal)
	if !added {
		cmd.Printf("Subject %s/%s already present in ClusterAuthorizationRule/%s (no change)\n", subjectKind, subjectPrincipal, ruleName)
		return nil
	}

	if err := unstructured.SetNestedSlice(obj.Object, newSubjects, "spec", "subjects"); err != nil {
		return fmt.Errorf("setting subjects: %w", err)
	}
	if _, err = client.Update(cmd.Context(), obj, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("updating ClusterAuthorizationRule %q: %w", ruleName, err)
	}
	cmd.Printf("Added subject %s/%s to ClusterAuthorizationRule/%s\n", subjectKind, subjectPrincipal, ruleName)
	return nil
}

func addSubjectToNamespacedRule(cmd *cobra.Command, dyn dynamic.Interface, ns, ruleName, subjectKind, subjectPrincipal string) error {
	client := dyn.Resource(authorizationRuleGVR).Namespace(ns)
	obj, err := client.Get(cmd.Context(), ruleName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting AuthorizationRule %q in namespace %q: %w", ruleName, ns, err)
	}

	newSubjects, added := addSubject(obj, subjectKind, subjectPrincipal)
	if !added {
		cmd.Printf("Subject %s/%s already present in AuthorizationRule/%s/%s (no change)\n", subjectKind, subjectPrincipal, ns, ruleName)
		return nil
	}

	if err := unstructured.SetNestedSlice(obj.Object, newSubjects, "spec", "subjects"); err != nil {
		return fmt.Errorf("setting subjects: %w", err)
	}
	if _, err = client.Update(cmd.Context(), obj, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("updating AuthorizationRule %s/%s: %w", ns, ruleName, err)
	}
	cmd.Printf("Added subject %s/%s to AuthorizationRule/%s/%s\n", subjectKind, subjectPrincipal, ns, ruleName)
	return nil
}

func addSubject(obj *unstructured.Unstructured, kind, name string) ([]any, bool) {
	subjects, _, _ := unstructured.NestedSlice(obj.Object, "spec", "subjects")
	for _, s := range subjects {
		sub, ok := s.(map[string]any)
		if !ok {
			continue
		}
		if fmt.Sprint(sub["kind"]) == kind && fmt.Sprint(sub["name"]) == name {
			return subjects, false
		}
	}
	return append(subjects, map[string]any{"kind": kind, "name": name}), true
}
