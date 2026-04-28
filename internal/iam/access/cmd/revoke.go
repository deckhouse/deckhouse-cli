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
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var revokeLong = templates.LongDesc(`
Revoke access previously granted with "d8 iam access grant".

The flags must match the original grant's scope/level — the d8-managed
object name is derived from the canonical spec, so a mismatch yields a
"not found" error rather than removing an unrelated rule.

Only d8-managed objects (label app.kubernetes.io/managed-by=d8-cli) are
affected. Manually maintained AuthorizationRules and
ClusterAuthorizationRules are never touched; edit them with kubectl.

© Flant JSC 2026`)

var revokeExample = templates.Examples(`
  # Revoke a namespaced grant (mirrors the grant command)
  d8 iam access revoke user anton --access-level Admin -n dev

  # Revoke a cluster-scoped grant
  d8 iam access revoke user anton --access-level ClusterAdmin --scope cluster

  # Revoke a labels-scoped grant (must match the original --scope value)
  d8 iam access revoke group admins --access-level Editor --scope labels=team=platform,tier=prod`)

func newRevokeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "revoke (user|group) NAME --access-level LEVEL [scope]",
		Short:             "Revoke a d8-managed access grant",
		Long:              revokeLong,
		Example:           revokeExample,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeSubjectAndName,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              runRevoke,
	}

	cmd.Flags().String("access-level", "", "Access level to revoke")
	cmd.Flags().StringSliceP("namespace", "n", nil, "Target namespace(s). Mutually exclusive with --scope")
	cmd.Flags().String("scope", "", "Cluster-wide scope: cluster | all-namespaces | labels=K=V[,K2=V2,...]. Mutually exclusive with -n/--namespace")
	cmd.Flags().Bool("port-forwarding", false, "Match grants with port-forwarding enabled")
	cmd.Flags().Bool("allow-scale", false, "Match grants with allow-scale enabled")

	_ = cmd.RegisterFlagCompletionFunc("access-level", completeAccessLevels)
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespacesFlag)
	_ = cmd.RegisterFlagCompletionFunc("scope", completeScopeFlag)

	return cmd
}

func runRevoke(cmd *cobra.Command, args []string) error {
	subjectKindStr := args[0]
	subjectName := args[1]
	accessLevel, _ := cmd.Flags().GetString("access-level")
	namespaces, _ := cmd.Flags().GetStringSlice("namespace")
	scopeFlag, _ := cmd.Flags().GetString("scope")
	portForwarding, _ := cmd.Flags().GetBool("port-forwarding")
	allowScale, _ := cmd.Flags().GetBool("allow-scale")

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

// revokeOpts mirrors grantOpts: it captures every parameter of a revoke so
// revokeManagedGrants doesn't need a 9-parameter signature.
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

// deleteManagedGrant deletes a d8-managed authorization rule. Refuses to
// touch objects that are not labelled as managed by d8-cli — manual cleanup
// of shared rules is intentionally outside this command's contract; use
// `kubectl edit` instead.
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
		return fmt.Errorf("%s is not managed by d8-cli; edit it manually with kubectl", formatRuleRef(kind, ns, name))
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
