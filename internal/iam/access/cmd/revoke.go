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
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var revokeLong = templates.LongDesc(`
Revoke access from a user or group.

There are two modes of operation:

SIMPLE MODE (recommended for most users):

  Revokes access that was previously granted with "d8 iam access grant".
  Uses the same flags to locate and delete the d8-managed object.

      d8 iam access revoke user NAME --access-level LEVEL [scope flags]

  Only d8-managed objects (label app.kubernetes.io/managed-by=d8-cli)
  are affected. Manual objects are never modified in this mode.

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
  d8 iam access revoke user anton --access-level ClusterAdmin --cluster

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
	cmd.Flags().StringSliceP("namespace", "n", nil, "Target namespace(s) (for intent-based revoke)")
	cmd.Flags().Bool("cluster", false, "Cluster-scoped revoke")
	cmd.Flags().Bool("all-namespaces", false, "All-namespaces scoped revoke")
	cmd.Flags().Bool("port-forwarding", false, "Match grants with port-forwarding enabled")
	cmd.Flags().Bool("allow-scale", false, "Match grants with allow-scale enabled")
	cmd.Flags().String("from", "", "Source object to revoke from: AuthorizationRule/<ns>/<name> or ClusterAuthorizationRule/<name>")
	cmd.Flags().Bool("delete-empty", false, "Delete the source object if subjects becomes empty (only with --from)")

	_ = cmd.RegisterFlagCompletionFunc("access-level", completeAccessLevels)
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespacesFlag)
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
		return fmt.Errorf("intent-based revoke requires: revoke (user|group) <name> --access-level <level> [scope]")
	}

	subjectKindStr := args[0]
	subjectName := args[1]
	accessLevel, _ := cmd.Flags().GetString("access-level")
	namespaces, _ := cmd.Flags().GetStringSlice("namespace")
	clusterFlag, _ := cmd.Flags().GetBool("cluster")
	allNSFlag, _ := cmd.Flags().GetBool("all-namespaces")
	portForwarding, _ := cmd.Flags().GetBool("port-forwarding")
	allowScale, _ := cmd.Flags().GetBool("allow-scale")

	if accessLevel == "" {
		return fmt.Errorf("--access-level is required for intent-based revoke")
	}

	subjectKind, err := parseSubjectKind(subjectKindStr)
	if err != nil {
		return err
	}

	scopeType, err := parseScopeFlags(namespaces, clusterFlag, allNSFlag)
	if err != nil {
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
	}

	switch scopeType {
	case "namespace":
		return revokeNamespaced(cmd, dyn, subjectKind, subjectName, subjectPrincipal, accessLevel, namespaces, allowScale, portForwarding)
	case "cluster", "all-namespaces":
		return revokeCluster(cmd, dyn, subjectKind, subjectName, subjectPrincipal, accessLevel, scopeType, allowScale, portForwarding)
	}
	return nil
}

func revokeNamespaced(cmd *cobra.Command, dyn dynamic.Interface,
	subjectKind, subjectRef, subjectPrincipal, accessLevel string,
	namespaces []string, allowScale, portForwarding bool) error {
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

		name, err := generateGrantName(spec)
		if err != nil {
			return err
		}

		client := dyn.Resource(authorizationRuleGVR).Namespace(ns)
		obj, err := client.Get(cmd.Context(), name, metav1.GetOptions{})
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Warning: d8-managed AuthorizationRule %q not found in namespace %q\n", name, ns)
			errs = multierror.Append(errs, fmt.Errorf("namespace %s: %w", ns, err))
			continue
		}

		labels := obj.GetLabels()
		if labels[managedByLabel] != managedByValue {
			return fmt.Errorf("AuthorizationRule %q in namespace %q is not managed by d8-cli; use --from to revoke from it explicitly", name, ns)
		}

		err = client.Delete(cmd.Context(), name, metav1.DeleteOptions{})
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to delete AuthorizationRule %q in %q: %v\n", name, ns, err)
			errs = multierror.Append(errs, fmt.Errorf("namespace %s: %w", ns, err))
			continue
		}
		cmd.Printf("Revoked: AuthorizationRule/%s/%s\n", ns, name)
	}

	return errs.ErrorOrNil()
}

func revokeCluster(cmd *cobra.Command, dyn dynamic.Interface,
	subjectKind, subjectRef, subjectPrincipal, accessLevel, scopeType string,
	allowScale, portForwarding bool) error {
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

	name, err := generateGrantName(spec)
	if err != nil {
		return err
	}

	client := dyn.Resource(clusterAuthorizationRuleGVR)
	obj, err := client.Get(cmd.Context(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("d8-managed ClusterAuthorizationRule %q not found: %w", name, err)
	}

	labels := obj.GetLabels()
	if labels[managedByLabel] != managedByValue {
		return fmt.Errorf("ClusterAuthorizationRule %q is not managed by d8-cli; use --from to revoke explicitly", name)
	}

	err = client.Delete(cmd.Context(), name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("deleting ClusterAuthorizationRule %q: %w", name, err)
	}

	cmd.Printf("Revoked: ClusterAuthorizationRule/%s\n", name)
	return nil
}

func runSourceBasedRevoke(cmd *cobra.Command, args []string) error {
	fromFlag, _ := cmd.Flags().GetString("from")
	deleteEmpty, _ := cmd.Flags().GetBool("delete-empty")

	if len(args) != 2 {
		return fmt.Errorf("source-based revoke requires: revoke --from <source> (user|group) <principal>")
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
	switch refKind {
	case "ClusterAuthorizationRule":
		return revokeSubjectFromClusterRule(cmd, dyn, refName, subjectKind, subjectName, deleteEmpty)
	case "AuthorizationRule":
		return revokeSubjectFromNamespacedRule(cmd, dyn, refNS, refName, subjectKind, subjectName, deleteEmpty)
	default:
		return fmt.Errorf("unsupported --from kind %q", refKind)
	}
}

func revokeSubjectFromClusterRule(cmd *cobra.Command, dyn dynamic.Interface, ruleName, subjectKind, subjectPrincipal string, deleteEmpty bool) error {
	client := dyn.Resource(clusterAuthorizationRuleGVR)
	obj, err := client.Get(cmd.Context(), ruleName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting ClusterAuthorizationRule %q: %w", ruleName, err)
	}

	newSubjects, removed := removeSubject(obj, subjectKind, subjectPrincipal)
	if !removed {
		return fmt.Errorf("subject %s/%s not found in ClusterAuthorizationRule %q", subjectKind, subjectPrincipal, ruleName)
	}

	if len(newSubjects) == 0 && deleteEmpty {
		err = client.Delete(cmd.Context(), ruleName, metav1.DeleteOptions{})
		if err != nil {
			return fmt.Errorf("deleting empty ClusterAuthorizationRule %q: %w", ruleName, err)
		}
		cmd.Printf("Deleted empty ClusterAuthorizationRule/%s\n", ruleName)
		return nil
	}

	if err := unstructured.SetNestedSlice(obj.Object, newSubjects, "spec", "subjects"); err != nil {
		return fmt.Errorf("setting subjects: %w", err)
	}
	_, err = client.Update(cmd.Context(), obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("updating ClusterAuthorizationRule %q: %w", ruleName, err)
	}
	cmd.Printf("Removed subject %s/%s from ClusterAuthorizationRule/%s\n", subjectKind, subjectPrincipal, ruleName)
	return nil
}

func revokeSubjectFromNamespacedRule(cmd *cobra.Command, dyn dynamic.Interface, ns, ruleName, subjectKind, subjectPrincipal string, deleteEmpty bool) error {
	client := dyn.Resource(authorizationRuleGVR).Namespace(ns)
	obj, err := client.Get(cmd.Context(), ruleName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting AuthorizationRule %q in namespace %q: %w", ruleName, ns, err)
	}

	newSubjects, removed := removeSubject(obj, subjectKind, subjectPrincipal)
	if !removed {
		return fmt.Errorf("subject %s/%s not found in AuthorizationRule %s/%s", subjectKind, subjectPrincipal, ns, ruleName)
	}

	if len(newSubjects) == 0 && deleteEmpty {
		err = client.Delete(cmd.Context(), ruleName, metav1.DeleteOptions{})
		if err != nil {
			return fmt.Errorf("deleting empty AuthorizationRule %s/%s: %w", ns, ruleName, err)
		}
		cmd.Printf("Deleted empty AuthorizationRule/%s/%s\n", ns, ruleName)
		return nil
	}

	if err := unstructured.SetNestedSlice(obj.Object, newSubjects, "spec", "subjects"); err != nil {
		return fmt.Errorf("setting subjects: %w", err)
	}
	_, err = client.Update(cmd.Context(), obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("updating AuthorizationRule %s/%s: %w", ns, ruleName, err)
	}
	cmd.Printf("Removed subject %s/%s from AuthorizationRule/%s/%s\n", subjectKind, subjectPrincipal, ns, ruleName)
	return nil
}

func removeSubject(obj *unstructured.Unstructured, kind, name string) ([]any, bool) {
	subjects, _, _ := unstructured.NestedSlice(obj.Object, "spec", "subjects")
	var newSubjects []any
	removed := false
	for _, s := range subjects {
		sub, ok := s.(map[string]any)
		if !ok {
			newSubjects = append(newSubjects, s)
			continue
		}
		if fmt.Sprint(sub["kind"]) == kind && fmt.Sprint(sub["name"]) == name {
			removed = true
			continue
		}
		newSubjects = append(newSubjects, s)
	}
	return newSubjects, removed
}
