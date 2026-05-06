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

For users, the subject is resolved by reading User.spec.email from the
cluster, because the current authz model requires email as the subject
name when user-authn is used.

Specify the scope via -n/--namespace OR --scope (mutually exclusive):

  -n/--namespace NS        AuthorizationRule per namespace. Repeat to target
                           several namespaces. User, PrivilegedUser, Editor,
                           Admin levels only.
  --scope cluster          ClusterAuthorizationRule, every namespace EXCEPT
                           system ones (d8-*, kube-system).
  --scope all-namespaces   ClusterAuthorizationRule with matchAny: true.
                           Covers ALL namespaces, including system ones.
  --scope labels=K=V[,...] ClusterAuthorizationRule with
                           namespaceSelector.labelSelector.matchLabels.

--allow-scale and --port-forwarding add capabilities to any scope.

© Flant JSC 2026`)

var grantExample = templates.Examples(`
  # Namespaced grants (one AR per --namespace)
  d8 iam access grant user anton --access-level Admin -n dev -n stage

  # Cluster-wide (no system namespaces)
  d8 iam access grant user anton --access-level ClusterAdmin --scope cluster

  # Cluster-wide including system namespaces
  d8 iam access grant user anton --access-level ClusterAdmin --scope all-namespaces

  # Match by namespace labels
  d8 iam access grant group admins --access-level Editor --scope labels=team=platform,tier=prod

  # Add port-forwarding capability
  d8 iam access grant group admins --access-level Editor -n dev --port-forwarding

  # Preview the manifest without applying
  d8 iam access grant user anton --access-level Admin -n dev --dry-run -o yaml`)

func newGrantCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "grant (user|group) NAME --access-level LEVEL [scope]",
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
	utilk8s.AddOutputFlag(cmd, "name", "name", "yaml", "json")

	_ = cmd.RegisterFlagCompletionFunc("access-level", completeAccessLevels)
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespacesFlag)
	_ = cmd.RegisterFlagCompletionFunc("scope", completeScopeFlag)

	return cmd
}

func runGrant(cmd *cobra.Command, args []string) error {
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
		return errors.New("--access-level is required")
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
		// Distinguish NotFound (legitimate — group may live in an external
		// provider, e.g. LDAP) from other errors (Forbidden, Timeout) that
		// would mislead the user into thinking the group is missing.
		if _, err := dyn.Resource(iamtypes.GroupGVR).Get(cmd.Context(), subjectName, metav1.GetOptions{}); err != nil {
			switch {
			case apierrors.IsNotFound(err):
				fmt.Fprintf(cmd.ErrOrStderr(), "Warning: local Group CR %q not found. Grant may target an external provider group.\n", subjectName)
			default:
				fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to look up local Group CR %q: %v\n", subjectName, err)
			}
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
