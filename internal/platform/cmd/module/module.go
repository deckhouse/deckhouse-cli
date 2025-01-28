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

package module

import (
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/enable"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/list"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/snapshots"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/values"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/disable"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/flags"
)

var moduleLong = templates.LongDesc(`
Module options for Deckhouse Kubernetes Platform.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	moduleCmd := &cobra.Command{
		Use: "module", Short: "Operate the Deckhouse Kubernetes Platform modules",
		Long: moduleLong,
	}

	moduleCmd.AddCommand(
		enable.NewCommand(),
		disable.NewCommand(),
		list.NewCommand(),
		values.NewCommand(),
		snapshots.NewCommand(),
	)

	flags.AddFlags(moduleCmd.Flags())

	return moduleCmd
}
