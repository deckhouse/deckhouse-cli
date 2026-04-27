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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var createExample = templates.Examples(`
  # Create a group
  d8 iam group create admins

  # Create and preview as YAML
  d8 iam group create admins --dry-run -o yaml`)

func newCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "create <name>",
		Short:         "Create a local group in Deckhouse",
		Example:       createExample,
		Args:          cobra.ExactArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			outputFmt, _ := cmd.Flags().GetString("output")

			obj := buildGroupObject(name)

			if dryRun {
				return utilk8s.PrintObject(cmd.OutOrStdout(), obj, outputFmt)
			}

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			created, err := dyn.Resource(iamtypes.GroupGVR).Create(cmd.Context(), obj, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("creating Group %q: %w", name, err)
			}

			return utilk8s.PrintObject(cmd.OutOrStdout(), created, outputFmt)
		},
	}

	cmd.Flags().Bool("dry-run", false, "Print the resource that would be created without applying")
	cmd.Flags().StringP("output", "o", "name", "Output format: name|yaml|json")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("name", "yaml", "json"))
	return cmd
}

func buildGroupObject(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": iamtypes.APIVersionDeckhouseV1Alpha1,
			// unstructured.GetKind() type-asserts the kind value to plain string;
			// cast the typed SubjectKind constant at this boundary.
			"kind": string(iamtypes.KindGroup),
			"metadata": map[string]any{
				"name": name,
			},
			"spec": map[string]any{
				"name":    name,
				"members": []any{},
			},
		},
	}
}
