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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/kubectl/pkg/util/templates"

	group "github.com/deckhouse/deckhouse-cli/internal/iam/group/cmd"
	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var createLong = templates.LongDesc(`
Create a local static user in Deckhouse (User CR).

The password can be provided interactively (--password-prompt), piped from stdin
(--password-stdin), auto-generated (--generate-password), or supplied as a
pre-computed bcrypt hash (--password-hash). If no password flag is given and
stdin is a terminal, the command prompts interactively.

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

  # Apply a pre-computed bcrypt hash
  d8 iam user create anton --email anton@abc.com --password-hash '$2y$10$abcdef...'

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
	addPasswordFlags(cmd)
	cmd.Flags().StringSlice("member-of", nil, "Add user to these groups (repeatable)")
	cmd.Flags().Bool("create-groups", false, "Create groups specified by --member-of if they do not exist")
	cmd.Flags().String("ttl", "", "User time-to-live (e.g. 24h, 30m). Can only be set once; expireAt will not update on change.")
	cmd.Flags().Bool("dry-run", false, "Print the resource that would be created without applying")
	utilk8s.AddOutputFlag(cmd, "name", "name", "yaml", "json")

	_ = cmd.RegisterFlagCompletionFunc("member-of", func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return utilk8s.CompleteResourceNames(cmd, iamtypes.GroupGVR, "", toComplete)
	})

	return cmd
}

func runCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

	email, _ := cmd.Flags().GetString("email")
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

	res, mode, err := resolvePasswordInput(cmd)
	if err != nil {
		return err
	}

	rawHash, err := res.rawBcryptHash()
	if err != nil {
		return err
	}
	// User.spec.password expects base64(<raw bcrypt hash>); the user-authn
	// hook (get_dex_user_crds) decodes it and forwards the bytes to the Dex
	// Password CR.
	encodedPassword := encodePasswordForUserCR(rawHash)

	obj := buildUserObject(name, email, encodedPassword, ttl)

	if dryRun {
		return utilk8s.PrintObject(cmd.OutOrStdout(), obj, outputFmt)
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	created, err := dyn.Resource(iamtypes.UserGVR).Create(cmd.Context(), obj, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating User %q: %w", name, err)
	}

	if err := utilk8s.PrintObject(cmd.OutOrStdout(), created, outputFmt); err != nil {
		return err
	}

	if mode == passwordModeGenerate {
		fmt.Fprintf(cmd.ErrOrStderr(), "Generated temporary password (shown once): %s\n", res.Plain)
		fmt.Fprintf(cmd.ErrOrStderr(), "Note: first login may require password change depending on cluster password policy.\n")
		fmt.Fprintf(cmd.ErrOrStderr(), "Note: if staticUsers2FA is enabled, the user will also need to enroll TOTP on first login.\n")
	}

	if len(memberOf) > 0 {
		var errs *multierror.Error
		for _, groupName := range memberOf {
			if _, err := group.EnsureMember(cmd.Context(), dyn, groupName, iamtypes.KindUser, name, group.EnsureMemberOpts{
				CreateGroupIfMissing: createGroups,
			}); err != nil {
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
			"apiVersion": iamtypes.APIVersionDeckhouseV1,
			// unstructured.GetKind() type-asserts the kind value to plain string,
			// so the typed SubjectKind constant must be cast at this boundary.
			"kind": string(iamtypes.KindUser),
			"metadata": map[string]any{
				"name": name,
			},
			"spec": spec,
		},
	}
}

func validateUserName(name string) error {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return fmt.Errorf("invalid user name %q: %s", name, strings.Join(errs, "; "))
	}
	return nil
}

func validateEmail(email string) error {
	if email == "" {
		return errors.New("--email is required")
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
