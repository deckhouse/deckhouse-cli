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

package network

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	cnimigration "github.com/deckhouse/deckhouse-cli/internal/network/cnimigration/cmd"
)

var networkLong = templates.LongDesc(`
A group of commands to operate network related tasks in The Deckhouse Ecosystem.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	networkCmd := &cobra.Command{
		Use:     "network",
		Short:   "A group of commands to operate network related tasks in The Deckhouse Ecosystem.",
		Aliases: []string{"n"},
		Long:    networkLong,
	}

	networkCmd.AddCommand(
		cnimigration.NewCommand(),
	)

	return networkCmd
}
