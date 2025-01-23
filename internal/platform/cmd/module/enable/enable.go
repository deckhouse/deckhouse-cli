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
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/operatemodule"

	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/edit/flags"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var enableLong = templates.LongDesc(`
Edit cluster-configuration in Kubernetes cluster.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	enableCmd := &cobra.Command{
		Use:           "enable",
		Short:         "Edit cluster-configuration.",
		Long:          enableLong,
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          enableModule,
	}
	flags.AddFlags(enableCmd.Flags())
	return enableCmd
}

func enableModule(cmd *cobra.Command, moduleName []string) error {
	err := operatemodule.OperateModule(cmd, moduleName[0], true)
	if err != nil {
		return fmt.Errorf("Error enable module: %w", err)
	}
	fmt.Printf("Module %s enabled", moduleName[0])
	return err
}
