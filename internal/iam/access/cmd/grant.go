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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
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
		return errors.New("--access-level is required (or use --to to add a subject to an existing rule)")
	}

	subjectKind, err := parseSubjectKind(subjectKindStr)
	if err != nil {
		return err
	}

	scopeType, err := parseScopeFlags(namespaces, clusterFlag, allNSFlag)
	if err != nil {
		return err
	}

	isNamespaced := scopeType == iamtypes.ScopeNamespace
	if err := validateAccessLevel(accessLevel, isNamespaced); err != nil {
		return err
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	var subjectPrincipal string
	switch subjectKind {
	case iamtypes.KindUser:
		subjectPrincipal, err = resolveUserEmail(cmd.Context(), dyn, subjectName)
		if err != nil {
			return err
		}
	case iamtypes.KindGroup:
		subjectPrincipal = subjectName
		if _, err := dyn.Resource(iamtypes.GroupGVR).Get(cmd.Context(), subjectName, metav1.GetOptions{}); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Warning: local Group CR %q not found. Grant may target an external provider group.\n", subjectName)
		}
	}

	opts := grantOpts{
		subjectKind:      subjectKind,
		subjectRef:       subjectName,
		subjectPrincipal: subjectPrincipal,
		accessLevel:      accessLevel,
		scopeType:        scopeType,
		namespaces:       namespaces,
		allowScale:       allowScale,
		portForwarding:   portForwarding,
		dryRun:           dryRun,
		outputFmt:        outputFmt,
	}
	return applyGrants(cmd, dyn, opts)
}

// grantOpts captures everything needed to materialize one or more grants. It
// exists so applyGrants doesn't take 10+ parameters and so the caller can be
// read top-to-bottom without arg-counting.
type grantOpts struct {
	subjectKind      iamtypes.SubjectKind
	subjectRef       string // local CR name (User.metadata.name or Group.metadata.name)
	subjectPrincipal string // value that ends up in spec.subjects[].name
	accessLevel      string
	scopeType        iamtypes.Scope
	namespaces       []string
	allowScale       bool
	portForwarding   bool
	dryRun           bool
	outputFmt        string
}

// applyGrants iterates every namespace for namespace-scoped grants and falls
// back to a single iteration for cluster-scoped ones. The resource client is
// chosen from the scope, and from then on the cluster vs namespaced paths are
// identical.
func applyGrants(cmd *cobra.Command, dyn dynamic.Interface, opts grantOpts) error {
	specs, err := canonicalGrantSpecs(canonicalGrantInput{
		SubjectKind:      opts.subjectKind,
		SubjectRef:       opts.subjectRef,
		SubjectPrincipal: opts.subjectPrincipal,
		AccessLevel:      opts.accessLevel,
		ScopeType:        opts.scopeType,
		Namespaces:       opts.namespaces,
		AllowScale:       opts.allowScale,
		PortForwarding:   opts.portForwarding,
	})
	if err != nil {
		return err
	}

	var errs *multierror.Error
	for _, spec := range specs {
		client, err := grantClient(dyn, spec)
		if err != nil {
			return err
		}
		obj, err := buildGrantObject(spec)
		if err != nil {
			return err
		}

		if opts.dryRun {
			if err := utilk8s.PrintObject(cmd.OutOrStdout(), obj, opts.outputFmt); err != nil {
				return err
			}
			continue
		}

		result, err := createOrUpdateGrant(cmd, client, obj, spec)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to create grant for %s: %v\n", grantHumanScope(spec), err)
			errs = multierror.Append(errs, fmt.Errorf("%s: %w", grantHumanScope(spec), err))
			continue
		}
		if err := utilk8s.PrintObject(cmd.OutOrStdout(), result, opts.outputFmt); err != nil {
			return err
		}
	}
	return errs.ErrorOrNil()
}

// canonicalGrantSpecs expands a canonicalGrantInput into one canonicalGrantSpec
// per concrete object that will be created. Namespaced scope produces N specs
// (one per namespace), cluster/all-namespaces produces a single spec. Used by
// both applyGrants and revokeManagedGrants so the two flows agree on object
// identity (since the d8-managed name is derived from the canonical spec).
func canonicalGrantSpecs(in canonicalGrantInput) ([]*canonicalGrantSpec, error) {
	base := &canonicalGrantSpec{
		Model:            iamtypes.ModelCurrent,
		SubjectKind:      in.SubjectKind,
		SubjectRef:       in.SubjectRef,
		SubjectPrincipal: in.SubjectPrincipal,
		AccessLevel:      in.AccessLevel,
		ScopeType:        in.ScopeType,
		AllowScale:       in.AllowScale,
		PortForwarding:   in.PortForwarding,
	}

	switch in.ScopeType {
	case iamtypes.ScopeNamespace:
		if len(in.Namespaces) == 0 {
			return nil, errors.New("namespaced grant requires at least one namespace")
		}
		specs := make([]*canonicalGrantSpec, 0, len(in.Namespaces))
		for _, ns := range in.Namespaces {
			if ns == "" {
				return nil, errors.New("namespace name must not be empty")
			}
			s := *base
			s.Namespaces = []string{ns}
			specs = append(specs, &s)
		}
		return specs, nil
	case iamtypes.ScopeCluster, iamtypes.ScopeAllNamespaces:
		return []*canonicalGrantSpec{base}, nil
	default:
		return nil, fmt.Errorf("unsupported scope %q", in.ScopeType)
	}
}

// grantClient picks the right dynamic.ResourceInterface for a spec. A
// namespaced spec has exactly one namespace at this point (canonicalSpecs
// guarantees that).
func grantClient(dyn dynamic.Interface, spec *canonicalGrantSpec) (dynamic.ResourceInterface, error) {
	switch spec.ScopeType {
	case iamtypes.ScopeNamespace:
		if len(spec.Namespaces) != 1 {
			return nil, fmt.Errorf("namespaced grant must target exactly one namespace, got %d", len(spec.Namespaces))
		}
		return dyn.Resource(iamtypes.AuthorizationRuleGVR).Namespace(spec.Namespaces[0]), nil
	case iamtypes.ScopeCluster, iamtypes.ScopeAllNamespaces:
		return dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR), nil
	default:
		return nil, fmt.Errorf("unsupported scope %q", spec.ScopeType)
	}
}

// buildGrantObject produces the Unstructured for a grant — AR for namespaced,
// CAR for cluster/all-namespaces. The object kind/apiVersion/namespace branch
// off ScopeType but the rest is shared.
func buildGrantObject(spec *canonicalGrantSpec) (*unstructured.Unstructured, error) {
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
				"kind": string(spec.SubjectKind),
				"name": spec.SubjectPrincipal,
			},
		},
	}

	metadata := map[string]any{
		"name":        name,
		"labels":      toAnyMap(labels),
		"annotations": toAnyMap(annotations),
	}

	var apiVersion, kind string
	switch spec.ScopeType {
	case iamtypes.ScopeNamespace:
		// canonicalGrantSpecs guarantees exactly one namespace per spec for
		// namespaced scope; recheck it here so this function stays correct
		// even if a future caller bypasses the expansion helper.
		if len(spec.Namespaces) != 1 {
			return nil, fmt.Errorf("namespaced grant must target exactly one namespace, got %d", len(spec.Namespaces))
		}
		if spec.Namespaces[0] == "" {
			return nil, errors.New("namespaced grant has empty namespace")
		}
		apiVersion = iamtypes.APIVersionDeckhouseV1Alpha1
		kind = iamtypes.KindAuthorizationRule
		metadata["namespace"] = spec.Namespaces[0]
	case iamtypes.ScopeCluster:
		apiVersion = iamtypes.APIVersionDeckhouseV1
		kind = iamtypes.KindClusterAuthorizationRule
	case iamtypes.ScopeAllNamespaces:
		apiVersion = iamtypes.APIVersionDeckhouseV1
		kind = iamtypes.KindClusterAuthorizationRule
		ruleSpec["namespaceSelector"] = map[string]any{"matchAny": true}
	default:
		return nil, fmt.Errorf("unsupported scope %q", spec.ScopeType)
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata":   metadata,
			"spec":       ruleSpec,
		},
	}, nil
}

// createOrUpdateGrant creates the object, or updates it if a same-named
// d8-managed object exists with a different canonical spec. Same canonical
// spec is treated as a no-op.
//
// A non-NotFound Get error is propagated rather than swallowed; otherwise a
// transient API failure (timeout, RBAC issue, conflict) would silently fall
// through to Create and produce a misleading "AlreadyExists" or — worse — a
// blind overwrite if the cached object differs from server state.
func createOrUpdateGrant(cmd *cobra.Command, client dynamic.ResourceInterface,
	obj *unstructured.Unstructured, spec *canonicalGrantSpec) (*unstructured.Unstructured, error) {
	name := obj.GetName()
	scope := grantHumanScope(spec)

	existing, err := client.Get(cmd.Context(), name, metav1.GetOptions{})
	switch {
	case err == nil:
		existingAnnot := existing.GetAnnotations()
		newAnnot := obj.GetAnnotations()
		if existingAnnot[iamtypes.AnnotationAccessCanonicalSpec] == newAnnot[iamtypes.AnnotationAccessCanonicalSpec] {
			cmd.PrintErrf("%s/%s unchanged\n", scope, name)
			return existing, nil
		}
		obj.SetResourceVersion(existing.GetResourceVersion())
		return client.Update(cmd.Context(), obj, metav1.UpdateOptions{})
	case apierrors.IsNotFound(err):
		return client.Create(cmd.Context(), obj, metav1.CreateOptions{})
	default:
		return nil, fmt.Errorf("getting %s/%s: %w", scope, name, err)
	}
}

// grantHumanScope renders the source kind (with optional namespace) used in
// progress and warning messages so output stays consistent with revoke.
func grantHumanScope(spec *canonicalGrantSpec) string {
	if spec.ScopeType == iamtypes.ScopeNamespace && len(spec.Namespaces) == 1 {
		return iamtypes.KindAuthorizationRule + "/" + spec.Namespaces[0]
	}
	return iamtypes.KindClusterAuthorizationRule
}

func parseSubjectKind(s string) (iamtypes.SubjectKind, error) {
	switch strings.ToLower(s) {
	case "user":
		return iamtypes.KindUser, nil
	case "group":
		return iamtypes.KindGroup, nil
	default:
		return "", fmt.Errorf("invalid subject kind %q: must be user or group", s)
	}
}

func parseScopeFlags(namespaces []string, cluster, allNamespaces bool) (iamtypes.Scope, error) {
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
		return "", errors.New("one of -n/--namespace, --cluster, or --all-namespaces must be specified")
	}
	if count > 1 {
		return "", errors.New("-n/--namespace, --cluster, and --all-namespaces are mutually exclusive")
	}

	if len(namespaces) > 0 {
		return iamtypes.ScopeNamespace, nil
	}
	if cluster {
		return iamtypes.ScopeCluster, nil
	}
	return iamtypes.ScopeAllNamespaces, nil
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
		return errors.New("source-based grant requires: grant --to <source> (user|group) <principal>")
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

	var client dynamic.ResourceInterface
	switch refKind {
	case iamtypes.KindClusterAuthorizationRule:
		client = dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR)
	case iamtypes.KindAuthorizationRule:
		client = dyn.Resource(iamtypes.AuthorizationRuleGVR).Namespace(refNS)
	default:
		return fmt.Errorf("unsupported --to kind %q", refKind)
	}
	return addSubjectToRule(cmd, client, refKind, refNS, refName, subjectKind, subjectPrincipal)
}

// addSubjectToRule adds (kind,principal) to spec.subjects of the rule pointed
// to by client. The cluster vs namespaced difference lives entirely in how
// the caller built the dynamic.ResourceInterface; ref/ns are only used for
// human messages.
func addSubjectToRule(cmd *cobra.Command, client dynamic.ResourceInterface,
	ruleKind, ns, ruleName string, subjectKind iamtypes.SubjectKind, subjectPrincipal string) error {
	ref := formatRuleRef(ruleKind, ns, ruleName)

	obj, err := client.Get(cmd.Context(), ruleName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting %s: %w", ref, err)
	}

	newSubjects, added := addSubject(obj, subjectKind, subjectPrincipal)
	if !added {
		cmd.Printf("Subject %s/%s already present in %s (no change)\n", subjectKind, subjectPrincipal, ref)
		return nil
	}

	if err := unstructured.SetNestedSlice(obj.Object, newSubjects, "spec", "subjects"); err != nil {
		return fmt.Errorf("setting subjects on %s: %w", ref, err)
	}
	if _, err := client.Update(cmd.Context(), obj, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("updating %s: %w", ref, err)
	}
	cmd.Printf("Added subject %s/%s to %s\n", subjectKind, subjectPrincipal, ref)
	return nil
}

func addSubject(obj *unstructured.Unstructured, kind iamtypes.SubjectKind, name string) ([]any, bool) {
	subjects, _, _ := unstructured.NestedSlice(obj.Object, "spec", "subjects")
	kindStr := string(kind)
	for _, s := range subjects {
		sub, ok := s.(map[string]any)
		if !ok {
			continue
		}
		if fmt.Sprint(sub["kind"]) == kindStr && fmt.Sprint(sub["name"]) == name {
			return subjects, false
		}
	}
	return append(subjects, map[string]any{"kind": kindStr, "name": name}), true
}
