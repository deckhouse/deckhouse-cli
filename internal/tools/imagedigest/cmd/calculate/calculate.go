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

package calculate

import (
	"encoding/hex"
	"fmt"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest"
)

var calculateLong = templates.LongDesc(`
Calculate GOST R 34.11-2012 (Streebog-256) digest for a container image.

The digest is calculated based on sorted layer digests but is NOT stored
in the image metadata. Use 'add' command to store the digest.

Example:
  d8 tools imagedigest calculate registry.example.com/image:tag
  d8 tools imagedigest calculate --insecure localhost:5000/image:latest`)

func NewCommand() *cobra.Command {
	calculateCmd := &cobra.Command{
		Use:           "calculate <image>",
		Short:         "Calculate GOST digest for a container image",
		Long:          calculateLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("this command requires exactly 1 argument (image reference), got %d", len(args))
			}
			return nil
		},
		RunE: runCalculate,
	}

	return calculateCmd
}

func runCalculate(cmd *cobra.Command, args []string) error {
	imageName := args[0]

	insecure, err := cmd.Flags().GetBool("insecure")
	if err != nil {
		return fmt.Errorf("failed to get insecure flag: %w", err)
	}

	var opts []crane.Option
	if insecure {
		opts = append(opts, crane.Insecure)
	}

	digest, err := imagedigest.CalculateGostImageDigest(imageName, opts...)
	if err != nil {
		return fmt.Errorf("failed to calculate GOST digest: %w", err)
	}

	fmt.Println(hex.EncodeToString(digest))

	return nil
}
