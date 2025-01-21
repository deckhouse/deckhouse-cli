/*
Copyright 2024 Flant JSC

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

package values

import (
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/edit/flags"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/operatemodule"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var valuesLong = templates.LongDesc(`
Module values by name in kubernetes cluster.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	listCmd := &cobra.Command{
		Use:           "values",
		Short:         "Module values.",
		Long:          valuesLong,
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          valuesModule,
	}
	flags.AddFlags(listCmd.Flags())
	return listCmd
}

func valuesModule(cmd *cobra.Command, moduleName []string) error {
	err := operatemodule.ValuesModule(cmd, moduleName[0])
	if err != nil {
		return fmt.Errorf("Error updating secret: %w", err)
	}
	return err
}
