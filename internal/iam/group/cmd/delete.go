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

package group

import (
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func newDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "delete <name>",
		Short:             "Delete a local group",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeGroupOnly,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			err = dyn.Resource(groupGVR).Delete(cmd.Context(), name, metav1.DeleteOptions{})
			if err != nil {
				return fmt.Errorf("deleting Group %q: %w", name, err)
			}

			cmd.Printf("Group %s deleted\n", name)
			return nil
		},
	}
}
