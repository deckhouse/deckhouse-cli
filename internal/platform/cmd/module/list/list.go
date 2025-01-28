/*
Copyright 2025 Flant JSC

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

package list

import (
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/operatemodule"

	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/edit/flags"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var listLong = templates.LongDesc(`
List enabled Deckhouse Kubernetes Platform modules.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	listCmd := &cobra.Command{
		Use:           "list",
		Short:         "List enabled modules.",
		Long:          listLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          listModule,
	}
	flags.AddFlags(listCmd.Flags())
	return listCmd
}

func listModule(cmd *cobra.Command, args []string) error {
	err := operatemodule.OptionsModule(cmd, "list.yaml")
	if err != nil {
		return fmt.Errorf("Error list modules: %w", err)
	}
	return err
}
