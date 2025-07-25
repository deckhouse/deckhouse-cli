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

package tools

import (
	"github.com/deckhouse/deckhouse-cli/internal/tools/farconverter"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var toolsLong = templates.LongDesc(`
Various useful tools for operating in The Deckhouse Ecosystem.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	toolsCmd := &cobra.Command{
		Use:     "tools",
		Short:   "Various useful tools for operating in The Deckhouse Ecosystem.",
		Aliases: []string{"t"},
		Long:    toolsLong,
	}

	toolsCmd.AddCommand(farconverter.NewCommand())

	return toolsCmd
}
