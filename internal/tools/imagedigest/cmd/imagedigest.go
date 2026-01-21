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

package cmd

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest/cmd/add"
	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest/cmd/calculate"
	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest/cmd/calculatefromfile"
	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest/cmd/validate"
)

var imagedigestLong = templates.LongDesc(`
Manage GOST R 34.11-2012 (Streebog) digests for container images.

This tool calculates GOST digests based on sorted layer digests of container images
and stores them in image annotations for integrity verification.

Available Commands:
  calculate            Calculate GOST digest for a container image
  calculate-from-file  Calculate GOST digest for a file
  add                  Calculate and add GOST digest to image metadata
  validate             Validate stored GOST digest against recalculated value

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	imagedigestCmd := &cobra.Command{
		Use:   "imagedigest",
		Short: "Manage GOST R 34.11-2012 (Streebog) digests for container images",
		Long:  imagedigestLong,
	}

	imagedigestCmd.PersistentFlags().BoolP("insecure", "i", false, "Allow insecure connections to registries (skip TLS verification)")

	imagedigestCmd.AddCommand(
		calculate.NewCommand(),
		calculatefromfile.NewCommand(),
		add.NewCommand(),
		validate.NewCommand(),
	)

	return imagedigestCmd
}
