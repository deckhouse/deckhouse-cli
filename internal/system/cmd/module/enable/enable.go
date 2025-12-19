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

package enable

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/operatemodule"
)

var enableLong = templates.LongDesc(`
Enable module using the ModuleConfig resource.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	enableCmd := &cobra.Command{
		Use:           "enable",
		Short:         "Enable module.",
		Long:          enableLong,
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          enableModule,
	}
	return enableCmd
}

func enableModule(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("this command requires exactly 1 argument: module name")
	}
	moduleName := args[0]

	dynamicClient, err := operatemodule.GetDynamicClient(cmd)
	if err != nil {
		return err
	}

	result, err := operatemodule.OperateModule(dynamicClient, moduleName, operatemodule.ModuleEnabled)
	if err != nil {
		var expErr *operatemodule.ExperimentalModuleError
		if errors.As(err, &expErr) {
			fmt.Fprintf(os.Stderr, "%s Module '%s' is experimental and cannot be enabled.\n", operatemodule.MsgError, expErr.ModuleName)
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, "To enable experimental modules, add the following to your 'deckhouse' ModuleConfig:")
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, "  spec:")
			fmt.Fprintln(os.Stderr, "    settings:")
			fmt.Fprintln(os.Stderr, "      allowExperimentalModules: true")
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, "Or run:")
			fmt.Fprintln(os.Stderr, "  kubectl patch mc deckhouse --type=merge -p '{\"spec\":{\"settings\":{\"allowExperimentalModules\":true}}}'")
			return fmt.Errorf("module '%s' is experimental", expErr.ModuleName)
		}
		return fmt.Errorf("failed to enable module: %w", err)
	}

	switch result.Status {
	case operatemodule.ResultAlreadyInState:
		fmt.Fprintf(os.Stderr, "%s Module '%s' is already enabled.\n", operatemodule.MsgWarn, moduleName)
	case operatemodule.ResultChanged:
		fmt.Printf("%s Module '%s' enabled.\n", operatemodule.MsgOK, moduleName)
	}

	return nil
}
