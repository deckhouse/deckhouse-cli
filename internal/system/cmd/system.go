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

package system

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	collectdebuginfo "github.com/deckhouse/deckhouse-cli/internal/system/cmd/collect-debug-info"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/edit"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/get"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/logs"
	module "github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/cmd"
	queue "github.com/deckhouse/deckhouse-cli/internal/system/cmd/queue"
	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

var systemLong = templates.LongDesc(`
Operate system options in DKP.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	systemCmd := &cobra.Command{
		Use:   "system",
		Short: "Operate system options. Aliases: s, p, platform",
		// TODO(mvasl) p and platform are old names of this commands and are left as aliases for backwards compatibility
		//  with our docs until we update them to use s or system.
		Aliases: []string{"s", "p", "platform"},
		Long:    systemLong,
		PreRunE: flags.ValidateParameters,
	}

	systemCmd.AddCommand(
		edit.NewCommand(),
		get.NewCommand(),
		module.NewCommand(),
		collectdebuginfo.NewCommand(),
		queue.NewCommand(),
		logs.NewCommand(),
	)

	flags.AddPersistentFlags(systemCmd)

	return systemCmd
}
