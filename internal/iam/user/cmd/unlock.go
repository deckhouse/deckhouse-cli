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
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func newUnlockCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "unlock <username>",
		Short:             "Unlock local user in Dex",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			username := args[0]
			wf, err := getWaitFlags(cmd)
			if err != nil {
				return err
			}

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			name := fmt.Sprintf("op-unlock-%d", time.Now().Unix())
			obj := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "deckhouse.io/v1",
					"kind":       "UserOperation",
					"metadata": map[string]any{
						"name": name,
					},
					"spec": map[string]any{
						"user":          username,
						"type":          "Unlock",
						"initiatorType": "admin",
					},
				},
			}

			_, err = createUserOperation(cmd.Context(), dyn, obj)
			if err != nil {
				return fmt.Errorf("create UserOperation: %w", err)
			}

			if !wf.wait {
				cmd.Printf("%s\n", name)
				return nil
			}

			result, err := waitUserOperation(cmd.Context(), dyn, name, wf.timeout)
			if err != nil {
				return fmt.Errorf("wait UserOperation: %w", err)
			}

			phase, _, _ := unstructured.NestedString(result.Object, "status", "phase")
			message, _, _ := unstructured.NestedString(result.Object, "status", "message")
			if phase == "Failed" {
				return fmt.Errorf("Unlock failed: %s", message)
			}
			cmd.Printf("Succeeded: %s\n", name)
			return nil
		},
	}

	cmd.Long = "Unlock local user in Dex.\n\nThis requests a UserOperation of type Unlock and waits for completion by default."
	addWaitFlags(cmd)
	return cmd
}
