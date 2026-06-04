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

package maintenance

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/cli"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/moduleconfig"
)

var maintenanceLong = templates.LongDesc(`
Enable or disable maintenance mode for a module via its ModuleConfig resource.

While maintenance mode is enabled, Deckhouse stops reconciling the module's
resources (it sets 'spec.maintenance="NoResourceReconciliation"' on the
ModuleConfig). This lets you develop or tweak the module's resources.

Disabling maintenance mode removes the 'spec.maintenance' field and lets
Deckhouse resume normal reconciliation.

The module's ModuleConfig must already exist. If it does not enable it first.

© Flant JSC 2026`)

var maintenanceEnableExample = templates.Examples(`
  # Enable maintenance mode
  d8 system module maintenance enable <module-name>
`)

var maintenanceDisableExample = templates.Examples(`
  # Disable maintenance mode
  d8 system module maintenance disable <module-name>
`)

func NewCommand() *cobra.Command {
	maintenanceCmd := &cobra.Command{
		Use:   "maintenance",
		Short: "Enable or disable module maintenance mode.",
		Long:  maintenanceLong,
	}

	maintenanceCmd.AddCommand(
		newSwitchCommand(
			"enable",
			"Enable maintenance mode for a module using ModuleConfig resource.",
			maintenanceEnableExample,
			moduleconfig.NoResourceReconciliation,
		),
		newSwitchCommand(
			"disable",
			"Disable maintenance mode for a module using ModuleConfig resource.",
			maintenanceDisableExample,
			moduleconfig.DefaultReconciliation,
		),
	)

	return maintenanceCmd
}

func newSwitchCommand(use, short, example string, state moduleconfig.MaintenanceState) *cobra.Command {
	return &cobra.Command{
		Use:           use + " <module-name>",
		Short:         short,
		Example:       example,
		Args:          cobra.ExactArgs(1),
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return switchMaintenance(cmd, args[0], state)
		},
	}
}

func switchMaintenance(cmd *cobra.Command, moduleName string, state moduleconfig.MaintenanceState) error {
	dynamicClient, err := cli.GetDynamicClient(cmd)
	if err != nil {
		return err
	}

	result, err := moduleconfig.SetMaintenanceState(dynamicClient, moduleName, state)
	if err != nil {
		if errors.IsNotFound(err) {
			fmt.Fprintf(os.Stderr, "%s ModuleConfig '%s' does not exist.\n", cli.MsgError, moduleName)
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, "Create it first by enabling the module:")
			fmt.Fprintf(os.Stderr, "  d8 system module enable %s\n\n", moduleName)

			return fmt.Errorf("module config '%s' does not exist", moduleName)
		}

		return fmt.Errorf("failed to switch maintenance mode: %w", err)
	}

	enabling := state == moduleconfig.NoResourceReconciliation

	switch result.Status {
	case moduleconfig.AlreadyInState:
		if enabling {
			fmt.Fprintf(os.Stderr, "%s Maintenance mode is already enabled for module '%s'.\n", cli.MsgInfo, moduleName)
		} else {
			fmt.Fprintf(os.Stderr, "%s Maintenance mode is already disabled for module '%s'.\n", cli.MsgInfo, moduleName)
		}
	case moduleconfig.Changed:
		if enabling {
			fmt.Printf("%s Maintenance mode enabled for module '%s'.\n", cli.MsgInfo, moduleName)
		} else {
			fmt.Printf("%s Maintenance mode disabled for module '%s'.\n", cli.MsgInfo, moduleName)
		}
	}

	return nil
}
