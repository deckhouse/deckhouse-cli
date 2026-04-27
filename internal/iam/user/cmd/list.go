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
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	sigsyaml "sigs.k8s.io/yaml"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func newListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "list",
		Short:         "List all local static users",
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			outputFmt, _ := cmd.Flags().GetString("output")

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			result, err := dyn.Resource(iamtypes.UserGVR).List(cmd.Context(), metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("listing Users: %w", err)
			}

			if len(result.Items) == 0 {
				cmd.Println("No users found")
				return nil
			}

			switch outputFmt {
			case "json":
				data, err := json.MarshalIndent(result, "", "  ")
				if err != nil {
					return fmt.Errorf("marshalling JSON: %w", err)
				}
				fmt.Fprintln(cmd.OutOrStdout(), string(data))
			case "yaml":
				data, err := sigsyaml.Marshal(result.UnstructuredContent())
				if err != nil {
					return fmt.Errorf("marshalling YAML: %w", err)
				}
				fmt.Fprint(cmd.OutOrStdout(), string(data))
			default:
				users := make([]*unstructured.Unstructured, 0, len(result.Items))
				for i := range result.Items {
					users = append(users, &result.Items[i])
				}
				return printUserTable(cmd, users)
			}
			return nil
		},
	}

	cmd.Flags().StringP("output", "o", "table", "Output format: table|json|yaml")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("table", "json", "yaml"))
	return cmd
}
