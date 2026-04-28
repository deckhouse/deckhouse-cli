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

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var revokeLong = templates.LongDesc(`
Revoke access from a user or group.

There are two modes of operation:

SIMPLE MODE (recommended for most users):

  Revokes access that was previously granted with "d8 iam access grant".
  Uses the same flags to locate and delete the d8-managed object.

      d8 iam access revoke user NAME --access-level LEVEL [-n NS... | --scope ...]

  Only d8-managed objects (label app.kubernetes.io/managed-by=d8-cli)
  are affected. Manual objects are never modified in this mode. The scope
  flags must match the original grant exactly (the d8-managed object name
  is derived from the canonical spec).

ADVANCED MODE (--from):

  Removes a subject from any existing authorization rule, even if it was
  not created by d8. Useful for shared rules or manual cleanup.

      d8 iam access revoke --from ClusterAuthorizationRule/RULE user PRINCIPAL
      d8 iam access revoke --from AuthorizationRule/NS/RULE  group GROUPNAME

  PRINCIPAL is matched literally against spec.subjects[].name — use the
  exact value from the rule (e.g. anton@example.com, not anton). No User
  CR resolution is performed in this mode.

  The rule is patched, not deleted. Use --delete-empty to delete the rule
  if no subjects remain after removal.

© Flant JSC 2026`)

var revokeExample = templates.Examples(`
  # Simple mode: revoke a namespaced grant (mirrors the grant command)
  d8 iam access revoke user anton --access-level Admin -n dev

  # Simple mode: revoke a cluster-scoped grant
  d8 iam access revoke user anton --access-level ClusterAdmin --scope cluster

  # Simple mode: revoke a labels-scoped grant (must match the original --scope value)
  d8 iam access revoke group admins --access-level Editor --scope labels=team=platform,tier=prod

  # Simple mode: revoke from a group
  d8 iam access revoke group admins --access-level Editor -n dev

  # Advanced mode: remove a user subject from a shared cluster rule (literal principal)
  d8 iam access revoke --from ClusterAuthorizationRule/my-rule user anton@example.com

  # Advanced mode: remove a group subject and delete the rule if it becomes empty
  d8 iam access revoke --from AuthorizationRule/dev/my-rule group admins --delete-empty`)

func newRevokeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "revoke (user|group) NAME [--access-level LEVEL scope | --from SOURCE]",
		Short:             "Revoke access from a user or group (current authz model)",
		Long:              revokeLong,
		Example:           revokeExample,
		ValidArgsFunction: completeSubjectAndName,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              runRevoke,
	}

	cmd.Flags().String("access-level", "", "Access level to revoke (for intent-based revoke)")
	cmd.Flags().StringSliceP("namespace", "n", nil, "Target namespace(s) (for intent-based revoke). Mutually exclusive with --scope")
	cmd.Flags().String("scope", "", "Cluster-wide scope: cluster | all-namespaces | labels=K=V[,K2=V2,...]. Mutually exclusive with -n/--namespace")
	cmd.Flags().Bool("port-forwarding", false, "Match grants with port-forwarding enabled")
	cmd.Flags().Bool("allow-scale", false, "Match grants with allow-scale enabled")
	cmd.Flags().String("from", "", "Source object to revoke from: AuthorizationRule/<ns>/<name> or ClusterAuthorizationRule/<name>")
	cmd.Flags().Bool("delete-empty", false, "Delete the source object if subjects becomes empty (only with --from)")

	_ = cmd.RegisterFlagCompletionFunc("access-level", completeAccessLevels)
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespacesFlag)
	_ = cmd.RegisterFlagCompletionFunc("scope", completeScopeFlag)
	_ = cmd.RegisterFlagCompletionFunc("from", completeRuleRef)

	return cmd
}

func runRevoke(cmd *cobra.Command, args []string) error {
	fromFlag, _ := cmd.Flags().GetString("from")

	if fromFlag != "" {
		return runSourceBasedRevoke(cmd, args)
	}
	return runIntentBasedRevoke(cmd, args)
}

func runIntentBasedRevoke(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return errors.New("intent-based revoke requires: revoke (user|group) <name> --access-level <level> [scope]")
	}

	subjectKindStr := args[0]
	subjectName := args[1]
	accessLevel, _ := cmd.Flags().GetString("access-level")
	namespaces, _ := cmd.Flags().GetStringSlice("namespace")
	scopeFlag, _ := cmd.Flags().GetString("scope")
	portForwarding, _ := cmd.Flags().GetBool("port-forwarding")
	allowScale, _ := cmd.Flags().GetBool("allow-scale")

	if accessLevel == "" {
		return errors.New("--access-level is required for intent-based revoke")
	}

	subjectKind, err := parseSubjectKind(subjectKindStr)
	if err != nil {
		return err
	}

	scopeType, scopeNS, labelMatch, err := parseScope(scopeFlag, namespaces)
	if err != nil {
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
	}

	opts := revokeOpts{
		subjectKind:      subjectKind,
		subjectRef:       subjectName,
		subjectPrincipal: subjectPrincipal,
		accessLevel:      accessLevel,
		scopeType:        scopeType,
		namespaces:       scopeNS,
		labelMatch:       labelMatch,
		allowScale:       allowScale,
		portForwarding:   portForwarding,
	}
	return revokeManagedGrants(cmd, dyn, opts)
}

// revokeOpts mirrors grantOpts: it captures every parameter of an
// intent-based revoke so revokeManagedGrants doesn't need a 9-parameter
// signature.
type revokeOpts struct {
	subjectKind      iamtypes.SubjectKind
	subjectRef       string
	subjectPrincipal string
	accessLevel      string
	scopeType        iamtypes.Scope
	namespaces       []string
	labelMatch       map[string]string
	allowScale       bool
	portForwarding   bool
}

// revokeManagedGrants is the inverse of applyGrants: it expands the opts into
// one canonical spec per concrete object, picks the right ResourceInterface
// for each, and deletes the d8-managed object via deleteManagedGrant.
func revokeManagedGrants(cmd *cobra.Command, dyn dynamic.Interface, opts revokeOpts) error {
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
		client, kind, ns, err := revokeClient(dyn, spec)
		if err != nil {
			return err
		}
		if err := deleteManagedGrant(cmd, client, spec, kind, ns); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs.ErrorOrNil()
}

// revokeClient returns the ResourceInterface plus the human-readable
// kind/namespace pair that deleteManagedGrant uses for messages and errors.
func revokeClient(dyn dynamic.Interface, spec *canonicalGrantSpec) (dynamic.ResourceInterface, string, string, error) {
	switch spec.ScopeType {
	case iamtypes.ScopeNamespace:
		if len(spec.Namespaces) != 1 {
			return nil, "", "", fmt.Errorf("namespaced revoke must target exactly one namespace, got %d", len(spec.Namespaces))
		}
		ns := spec.Namespaces[0]
		return dyn.Resource(iamtypes.AuthorizationRuleGVR).Namespace(ns), iamtypes.KindAuthorizationRule, ns, nil
	case iamtypes.ScopeCluster, iamtypes.ScopeAllNamespaces, iamtypes.ScopeLabels:
		return dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR), iamtypes.KindClusterAuthorizationRule, "", nil
	default:
		return nil, "", "", fmt.Errorf("unsupported scope %q", spec.ScopeType)
	}
}

// deleteManagedGrant deletes a d8-managed authorization rule (cluster or
// namespaced — the caller picks the dynamic.ResourceInterface). Refuses to
// touch objects that are not labelled as managed by d8-cli; that error path
// is what tells users to switch to --from for manual cleanup.
func deleteManagedGrant(cmd *cobra.Command, client dynamic.ResourceInterface,
	spec *canonicalGrantSpec, kind, ns string) error {
	name, err := generateGrantName(spec)
	if err != nil {
		return err
	}

	obj, err := client.Get(cmd.Context(), name, metav1.GetOptions{})
	if err != nil {
		ref := formatRuleRef(kind, ns, name)
		fmt.Fprintf(cmd.ErrOrStderr(), "Warning: d8-managed %s not found\n", ref)
		return fmt.Errorf("%s: %w", ref, err)
	}

	if obj.GetLabels()[iamtypes.LabelManagedBy] != iamtypes.ManagedByValueCLI {
		return fmt.Errorf("%s is not managed by d8-cli; use --from to revoke from it explicitly", formatRuleRef(kind, ns, name))
	}

	if err := client.Delete(cmd.Context(), name, metav1.DeleteOptions{}); err != nil {
		ref := formatRuleRef(kind, ns, name)
		fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to delete %s: %v\n", ref, err)
		return fmt.Errorf("%s: %w", ref, err)
	}

	cmd.Printf("Revoked: %s\n", formatRuleRef(kind, ns, name))
	return nil
}

// formatRuleRef formats a ruleRef the way users see it ("Kind/Name" or
// "Kind/NS/Name"), shared between revoke output and error messages so they
// always agree.
func formatRuleRef(kind, ns, name string) string {
	if ns == "" {
		return fmt.Sprintf("%s/%s", kind, name)
	}
	return fmt.Sprintf("%s/%s/%s", kind, ns, name)
}

func runSourceBasedRevoke(cmd *cobra.Command, args []string) error {
	fromFlag, _ := cmd.Flags().GetString("from")
	deleteEmpty, _ := cmd.Flags().GetBool("delete-empty")

	if len(args) != 2 {
		return errors.New("source-based revoke requires: revoke --from <source> (user|group) <principal>")
	}
	subjectKind, err := parseSubjectKind(args[0])
	if err != nil {
		return err
	}
	// Principal is used literally — it must match spec.subjects[].name exactly.
	// No User CR lookup here, because --from targets rules that may reference
	// external identities or non-d8-managed subjects.
	subjectName := args[1]

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	refKind, refNS, refName, err := parseRuleRef(fromFlag)
	if err != nil {
		return fmt.Errorf("invalid --from: %w", err)
	}

	var client dynamic.ResourceInterface
	switch refKind {
	case iamtypes.KindClusterAuthorizationRule:
		client = dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR)
	case iamtypes.KindAuthorizationRule:
		client = dyn.Resource(iamtypes.AuthorizationRuleGVR).Namespace(refNS)
	default:
		return fmt.Errorf("unsupported --from kind %q", refKind)
	}
	return revokeSubjectFromRule(cmd, client, refKind, refNS, refName, subjectKind, subjectName, deleteEmpty)
}

// revokeSubjectFromRule removes a single subject from any AR or CAR. The
// dynamic.ResourceInterface argument is what makes the cluster vs namespaced
// difference disappear; the kind/ns are kept around purely for human-readable
// messages.
func revokeSubjectFromRule(cmd *cobra.Command, client dynamic.ResourceInterface,
	ruleKind, ns, ruleName string, subjectKind iamtypes.SubjectKind, subjectPrincipal string, deleteEmpty bool) error {
	ref := formatRuleRef(ruleKind, ns, ruleName)

	obj, err := client.Get(cmd.Context(), ruleName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting %s: %w", ref, err)
	}

	newSubjects, removed := removeSubject(obj, subjectKind, subjectPrincipal)
	if !removed {
		return fmt.Errorf("subject %s/%s not found in %s", subjectKind, subjectPrincipal, ref)
	}

	if len(newSubjects) == 0 && deleteEmpty {
		if err := client.Delete(cmd.Context(), ruleName, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("deleting empty %s: %w", ref, err)
		}
		cmd.Printf("Deleted empty %s\n", ref)
		return nil
	}

	if err := unstructured.SetNestedSlice(obj.Object, newSubjects, "spec", "subjects"); err != nil {
		return fmt.Errorf("setting subjects on %s: %w", ref, err)
	}
	if _, err := client.Update(cmd.Context(), obj, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("updating %s: %w", ref, err)
	}
	cmd.Printf("Removed subject %s/%s from %s\n", subjectKind, subjectPrincipal, ref)
	return nil
}

func removeSubject(obj *unstructured.Unstructured, kind iamtypes.SubjectKind, name string) ([]any, bool) {
	subjects, _, _ := unstructured.NestedSlice(obj.Object, "spec", "subjects")
	kindStr := string(kind)
	var newSubjects []any
	removed := false
	for _, s := range subjects {
		sub, ok := s.(map[string]any)
		if !ok {
			newSubjects = append(newSubjects, s)
			continue
		}
		if fmt.Sprint(sub["kind"]) == kindStr && fmt.Sprint(sub["name"]) == name {
			removed = true
			continue
		}
		newSubjects = append(newSubjects, s)
	}
	return newSubjects, removed
}
