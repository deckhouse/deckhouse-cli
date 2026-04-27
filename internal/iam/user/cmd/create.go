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

package user

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var userGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1",
	Resource: "users",
}

var groupGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "groups",
}

var createLong = templates.LongDesc(`
Create a local static user in Deckhouse (User CR).

The password can be provided interactively (--password-prompt), piped from stdin
(--password-stdin), or auto-generated (--generate-password). If no password flag
is given and stdin is a terminal, the command prompts interactively.

When --generate-password is used, the password is shown exactly once on stderr.

If the cluster has a password policy enabled, the user may be required to change
the password on first login. If staticUsers2FA is enabled, the user will also
need to enroll TOTP on first login.

© Flant JSC 2026`)

var createExample = templates.Examples(`
  # Create a user with interactive password prompt
  d8 iam user create anton --email anton@abc.com

  # Create a user with auto-generated password
  d8 iam user create anton --email anton@abc.com --generate-password

  # Create a user from a CI pipeline
  echo "s3cret" | d8 iam user create anton --email anton@abc.com --password-stdin

  # Create a user and add to groups
  d8 iam user create anton --email anton@abc.com --generate-password --member-of admins --create-groups

  # Create a temporary user with TTL
  d8 iam user create anton --email anton@abc.com --generate-password --ttl 24h`)

func newCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "create <name> --email <email> [flags]",
		Short:         "Create a local static user in Deckhouse",
		Long:          createLong,
		Example:       createExample,
		Args:          cobra.ExactArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          runCreate,
	}

	cmd.Flags().String("email", "", "User email address (required, must be lowercase)")
	_ = cmd.MarkFlagRequired("email")
	cmd.Flags().Bool("password-prompt", false, "Read password interactively with hidden input")
	cmd.Flags().Bool("password-stdin", false, "Read password from stdin (for CI/pipelines)")
	cmd.Flags().Bool("generate-password", false, "Auto-generate a strong password (shown once on stderr)")
	cmd.Flags().StringSlice("member-of", nil, "Add user to these groups (repeatable)")
	cmd.Flags().Bool("create-groups", false, "Create groups specified by --member-of if they do not exist")
	cmd.Flags().String("ttl", "", "User time-to-live (e.g. 24h, 30m). Can only be set once; expireAt will not update on change.")
	cmd.Flags().Bool("dry-run", false, "Print the resource that would be created without applying")
	cmd.Flags().StringP("output", "o", "name", "Output format: name|yaml|json")

	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("name", "yaml", "json"))
	_ = cmd.RegisterFlagCompletionFunc("member-of", func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return utilk8s.CompleteResourceNames(cmd, groupGVR, "", toComplete)
	})

	return cmd
}

func runCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

	email, _ := cmd.Flags().GetString("email")
	promptFlag, _ := cmd.Flags().GetBool("password-prompt")
	stdinFlag, _ := cmd.Flags().GetBool("password-stdin")
	generateFlag, _ := cmd.Flags().GetBool("generate-password")
	memberOf, _ := cmd.Flags().GetStringSlice("member-of")
	createGroups, _ := cmd.Flags().GetBool("create-groups")
	ttl, _ := cmd.Flags().GetString("ttl")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	outputFmt, _ := cmd.Flags().GetString("output")

	if err := validateUserName(name); err != nil {
		return err
	}
	if err := validateEmail(email); err != nil {
		return err
	}
	if ttl != "" {
		if err := validateTTL(ttl); err != nil {
			return err
		}
	}

	if strings.HasPrefix(name, "system:") {
		fmt.Fprintf(cmd.ErrOrStderr(), "Warning: users with \"system:\" prefix may be rejected by the cluster\n")
	}

	mode, err := resolvePasswordMode(promptFlag, stdinFlag, generateFlag)
	if err != nil {
		return err
	}

	var plainPassword string
	switch mode {
	case passwordModePrompt:
		plainPassword, err = readPasswordPrompt(int(os.Stdin.Fd()), cmd.ErrOrStderr())
	case passwordModeStdin:
		plainPassword, err = readPasswordStdin(os.Stdin)
	case passwordModeGenerate:
		plainPassword, err = generatePassword()
	}
	if err != nil {
		return err
	}

	encodedPassword, err := encodePasswordForDeckhouse(plainPassword)
	if err != nil {
		return err
	}

	obj := buildUserObject(name, email, encodedPassword, ttl)

	if dryRun {
		return utilk8s.PrintObject(cmd.OutOrStdout(), obj, outputFmt)
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	created, err := dyn.Resource(userGVR).Create(cmd.Context(), obj, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating User %q: %w", name, err)
	}

	if err := utilk8s.PrintObject(cmd.OutOrStdout(), created, outputFmt); err != nil {
		return err
	}

	if mode == passwordModeGenerate {
		fmt.Fprintf(cmd.ErrOrStderr(), "Generated temporary password (shown once): %s\n", plainPassword)
		fmt.Fprintf(cmd.ErrOrStderr(), "Note: first login may require password change depending on cluster password policy.\n")
		fmt.Fprintf(cmd.ErrOrStderr(), "Note: if staticUsers2FA is enabled, the user will also need to enroll TOTP on first login.\n")
	}

	if len(memberOf) > 0 {
		var errs *multierror.Error
		for _, groupName := range memberOf {
			if err := ensureGroupMember(cmd, dyn, groupName, "User", name, createGroups); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to add user to group %q: %v\n", groupName, err)
				errs = multierror.Append(errs, err)
			}
		}
		if errs.ErrorOrNil() != nil {
			return fmt.Errorf("User %s created, but failed to update %d group(s): %w",
				name, errs.Len(), errs)
		}
	}

	return nil
}

func buildUserObject(name, email, password, ttl string) *unstructured.Unstructured {
	spec := map[string]any{
		"email":    email,
		"password": password,
	}
	if ttl != "" {
		spec["ttl"] = ttl
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1",
			"kind":       "User",
			"metadata": map[string]any{
				"name": name,
			},
			"spec": spec,
		},
	}
}

func ensureGroupMember(cmd *cobra.Command, dyn dynamic.Interface, groupName, memberKind, memberName string, createIfMissing bool) error {
	ctx := cmd.Context()
	groupClient := dyn.Resource(groupGVR)

	obj, err := groupClient.Get(ctx, groupName, metav1.GetOptions{})
	if err != nil {
		if !createIfMissing {
			return fmt.Errorf("group %q not found: %w", groupName, err)
		}
		obj = &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "deckhouse.io/v1alpha1",
				"kind":       "Group",
				"metadata": map[string]any{
					"name": groupName,
				},
				"spec": map[string]any{
					"name": groupName,
					"members": []any{
						map[string]any{
							"kind": memberKind,
							"name": memberName,
						},
					},
				},
			},
		}
		_, err = groupClient.Create(ctx, obj, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("creating group %q: %w", groupName, err)
		}
		return nil
	}

	members, _, _ := unstructured.NestedSlice(obj.Object, "spec", "members")
	for _, m := range members {
		member, ok := m.(map[string]any)
		if !ok {
			continue
		}
		if member["kind"] == memberKind && member["name"] == memberName {
			return nil // already a member
		}
	}

	members = append(members, map[string]any{
		"kind": memberKind,
		"name": memberName,
	})
	if err := unstructured.SetNestedSlice(obj.Object, members, "spec", "members"); err != nil {
		return fmt.Errorf("setting members: %w", err)
	}

	_, err = groupClient.Update(ctx, obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("updating group %q: %w", groupName, err)
	}
	return nil
}

func validateUserName(name string) error {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return fmt.Errorf("invalid user name %q: %s", name, strings.Join(errs, "; "))
	}
	return nil
}

func validateEmail(email string) error {
	if email == "" {
		return fmt.Errorf("--email is required")
	}
	if email != strings.ToLower(email) {
		return fmt.Errorf("email must be lowercase, got %q", email)
	}
	if !strings.Contains(email, "@") {
		return fmt.Errorf("email %q does not look like a valid email address", email)
	}
	return nil
}

func validateTTL(ttl string) error {
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return fmt.Errorf("invalid --ttl %q: %w (expected like 24h, 30m, 1h30m)", ttl, err)
	}
	if d <= 0 {
		return fmt.Errorf("invalid --ttl %q: must be positive", ttl)
	}
	return nil
}
