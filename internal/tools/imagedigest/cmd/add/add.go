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

package add

import (
	"fmt"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest"
)

func NewCommand() *cobra.Command {
	addCmd := &cobra.Command{
		Use:   "add <image>",
		Short: "Calculating and adding the image digest to the image metadata according to the GOST standard Streebog (GOST R 34.11-2012)",
		Long:  `Calculating and adding the image digest to the image metadata according to the GOST standard Streebog (GOST R 34.11-2012)`,
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
				return err
			}
			return nil
		},
		RunE: runAdd,
	}

	return addCmd
}

func runAdd(cmd *cobra.Command, args []string) error {
	imageName := args[0]

	insecure, err := cmd.Flags().GetBool("insecure")
	if err != nil {
		return fmt.Errorf("failed to get insecure flag: %w", err)
	}

	var opts []crane.Option
	if insecure {
		opts = append(opts, crane.Insecure)
	}

	digest, err := imagedigest.PullAnnotatePush(imageName, opts...)
	if err != nil {
		log.Fatal().Err(err).Msg("AddGostImageDigest")
	}

	log.Info().Msgf("GOST Image Digest: %s", digest)
	log.Info().Msg("Added successfully")

	return nil
}
