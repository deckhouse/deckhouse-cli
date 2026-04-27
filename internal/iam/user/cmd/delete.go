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

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var deleteLong = templates.LongDesc(`
Delete a local static user from Deckhouse.

This removes the User CR. It does not automatically remove the user from groups.
You may want to run "d8 iam group remove-member" separately.

© Flant JSC 2026`)

func newDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "delete <name>",
		Short:             "Delete a local static user",
		Long:              deleteLong,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			err = dyn.Resource(iamtypes.UserGVR).Delete(cmd.Context(), name, metav1.DeleteOptions{})
			if err != nil {
				return fmt.Errorf("deleting User %q: %w", name, err)
			}

			fmt.Fprintf(cmd.ErrOrStderr(), "Warning: user %q may still be referenced in Group memberships. Use \"d8 iam group remove-member\" to clean up.\n", name)
			cmd.Printf("User %s deleted\n", name)
			return nil
		},
	}
	return cmd
}
