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

Specify the scope via -n/--namespace OR --scope (mutually exclusive):

  -n/--namespace NS   Creates an AuthorizationRule per namespace (namespaced scope).
                      Repeat to target several namespaces at once.
                      Only User, PrivilegedUser, Editor, Admin levels are valid.

  --scope cluster     Creates a ClusterAuthorizationRule. The grant applies to
                      every namespace EXCEPT system ones (d8-*, kube-system).
                      All access levels are valid, including
                      ClusterEditor / ClusterAdmin / SuperAdmin.

  --scope all-namespaces
                      Creates a ClusterAuthorizationRule with
                      namespaceSelector.matchAny: true. Covers ALL namespaces
                      including system ones (d8-system, kube-system, ...).
                      Use with caution.

  --scope labels=K=V[,K2=V2,...]
                      Creates a ClusterAuthorizationRule with
                      namespaceSelector.labelSelector.matchLabels = {K: V, ...}.
                      Targets every namespace matching all of the given labels.

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
  d8 iam access grant user anton --access-level ClusterAdmin --scope cluster

  # Grant ClusterAdmin to ALL namespaces including d8-system, kube-system
  d8 iam access grant user anton --access-level ClusterAdmin --scope all-namespaces

  # Grant Editor only in namespaces labelled team=platform,tier=prod
  d8 iam access grant group admins --access-level Editor --scope labels=team=platform,tier=prod

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
	cmd.Flags().StringSliceP("namespace", "n", nil, "Target namespace(s) for namespaced grants (repeatable). Mutually exclusive with --scope")
	cmd.Flags().String("scope", "", "Cluster-wide scope: cluster | all-namespaces | labels=K=V[,K2=V2,...]. Mutually exclusive with -n/--namespace")
	cmd.Flags().Bool("port-forwarding", false, "Allow port-forwarding")
	cmd.Flags().Bool("allow-scale", false, "Allow scaling workloads")
	cmd.Flags().Bool("dry-run", false, "Print the resource(s) that would be created without applying")
	cmd.Flags().StringP("output", "o", "name", "Output format: name|yaml|json")
	cmd.Flags().String("to", "", "Existing rule to add the subject to: AuthorizationRule/<ns>/<name> or ClusterAuthorizationRule/<name>")

	_ = cmd.RegisterFlagCompletionFunc("access-level", completeAccessLevels)
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespacesFlag)
	_ = cmd.RegisterFlagCompletionFunc("scope", completeScopeFlag)
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
	scopeFlag, _ := cmd.Flags().GetString("scope")
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

	scopeType, scopeNS, labelMatch, err := parseScope(scopeFlag, namespaces)
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
		namespaces:       scopeNS,
		labelMatch:       labelMatch,
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
	labelMatch       map[string]string
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
		LabelMatch:       opts.labelMatch,
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
// (one per namespace), cluster/all-namespaces/labels produce a single spec.
// Used by both applyGrants and revokeManagedGrants so the two flows agree on
// object identity (since the d8-managed name is derived from the canonical spec).
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
	case iamtypes.ScopeLabels:
		if len(in.LabelMatch) == 0 {
			return nil, errors.New("labels scope requires at least one K=V pair")
		}
		s := *base
		// copy to avoid sharing the caller's map
		s.LabelMatch = make(map[string]string, len(in.LabelMatch))
		for k, v := range in.LabelMatch {
			s.LabelMatch[k] = v
		}
		return []*canonicalGrantSpec{&s}, nil
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
	case iamtypes.ScopeCluster, iamtypes.ScopeAllNamespaces, iamtypes.ScopeLabels:
		return dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR), nil
	default:
		return nil, fmt.Errorf("unsupported scope %q", spec.ScopeType)
	}
}

// buildGrantObject produces the Unstructured for a grant — AR for namespaced,
// CAR for cluster/all-namespaces/labels. The object kind/apiVersion/namespace
// branch off ScopeType but the rest is shared.
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
	case iamtypes.ScopeLabels:
		if len(spec.LabelMatch) == 0 {
			return nil, errors.New("labels scope requires at least one label pair")
		}
		apiVersion = iamtypes.APIVersionDeckhouseV1
		kind = iamtypes.KindClusterAuthorizationRule
		matchLabels := make(map[string]any, len(spec.LabelMatch))
		for k, v := range spec.LabelMatch {
			matchLabels[k] = v
		}
		ruleSpec["namespaceSelector"] = map[string]any{
			"labelSelector": map[string]any{
				"matchLabels": matchLabels,
			},
		}
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

// parseScope translates the user-facing -n/--namespace and --scope flags into
// a canonical (Scope, namespaces, labelMatch) triple. Exactly one of the two
// flag sets must be set.
func parseScope(scope string, namespaces []string) (iamtypes.Scope, []string, map[string]string, error) {
	hasNS := len(namespaces) > 0
	hasScope := scope != ""

	switch {
	case hasNS && hasScope:
		return "", nil, nil, errors.New("-n/--namespace and --scope are mutually exclusive")
	case !hasNS && !hasScope:
		return "", nil, nil, errors.New("one of -n/--namespace or --scope must be specified")
	case hasNS:
		return iamtypes.ScopeNamespace, namespaces, nil, nil
	}

	switch {
	case scope == "cluster":
		return iamtypes.ScopeCluster, nil, nil, nil
	case scope == "all-namespaces", scope == "all":
		return iamtypes.ScopeAllNamespaces, nil, nil, nil
	case strings.HasPrefix(scope, "labels="):
		labels, err := parseLabelMatch(strings.TrimPrefix(scope, "labels="))
		if err != nil {
			return "", nil, nil, fmt.Errorf("--scope: %w", err)
		}
		return iamtypes.ScopeLabels, nil, labels, nil
	default:
		return "", nil, nil, fmt.Errorf("invalid --scope %q: expected one of cluster, all-namespaces, labels=K=V[,K2=V2,...]", scope)
	}
}

// parseLabelMatch parses "K=V[,K2=V2,...]" into a deterministic map. Empty
// keys, empty values and duplicate keys are rejected so the resulting
// labelSelector cannot be ambiguous.
func parseLabelMatch(s string) (map[string]string, error) {
	if s == "" {
		return nil, errors.New("labels=... must contain at least one K=V pair")
	}
	out := make(map[string]string)
	for _, pair := range strings.Split(s, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid label pair %q: expected key=value", pair)
		}
		k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		if k == "" || v == "" {
			return nil, fmt.Errorf("invalid label pair %q: key and value must be non-empty", pair)
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("duplicate label key %q", k)
		}
		out[k] = v
	}
	return out, nil
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
		"access-level": ruleSpecHint,
		"namespace":    ruleSpecHint,
		"scope":        ruleSpecHint,
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
