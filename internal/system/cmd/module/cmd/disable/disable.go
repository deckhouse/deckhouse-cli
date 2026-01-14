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

package disable

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/cli"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/moduleconfig"
)

var disableLong = templates.LongDesc(`
Disable module using the ModuleConfig resource.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	disableCmd := &cobra.Command{
		Use:           "disable",
		Short:         "Disable module.",
		Long:          disableLong,
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          disableModule,
	}
	return disableCmd
}

func disableModule(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("this command requires exactly 1 argument: module name")
	}
	moduleName := args[0]

	dynamicClient, err := cli.GetDynamicClient(cmd)
	if err != nil {
		return err
	}

	result, err := moduleconfig.SetEnabledState(dynamicClient, moduleName, moduleconfig.Disabled)
	if err != nil {
		return fmt.Errorf("failed to disable module: %w", err)
	}

	switch result.Status {
	case moduleconfig.AlreadyInState:
		fmt.Fprintf(os.Stderr, "%s Module '%s' is already disabled.\n", cli.MsgInfo, moduleName)
	case moduleconfig.Changed:
		fmt.Printf("%s Module '%s' disabled.\n", cli.MsgInfo, moduleName)
	}

	return nil
}
