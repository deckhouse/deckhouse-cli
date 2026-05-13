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

package basic

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

func NewConfigCmd(opts *registry.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "config IMAGE",
		Short: "Print the config of an image",
		Long: `Print the raw config JSON of an image to stdout. Multi-arch indices are
resolved to a single image via --platform.`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion.ImageRef(),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := registry.FetchConfig(cmd.Context(), args[0], opts)
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(data)
			return err
		},
	}
}
