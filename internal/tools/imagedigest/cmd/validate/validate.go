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

package validate

import (
	"fmt"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest"
)

func NewCommand() *cobra.Command {
	validateCmd := &cobra.Command{
		Use:   "validate <image>",
		Short: "Validating the image digest in the image metadata calculated according to the GOST standard Streebog (GOST R 34.11-2012)",
		Long:  `Validating the image digest in the image metadata calculated according to the GOST standard Streebog (GOST R 34.11-2012)`,
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
				return err
			}
			return nil
		},
		RunE: runValidate,
	}

	validateCmd.Flags().Bool("fix", false, "Fix GOST Image Digest if it is incorrect")

	return validateCmd
}

func runValidate(cmd *cobra.Command, args []string) error {
	imageName := args[0]

	insecure, err := cmd.Flags().GetBool("insecure")
	if err != nil {
		return fmt.Errorf("failed to get insecure flag: %w", err)
	}

	fix, err := cmd.Flags().GetBool("fix")
	if err != nil {
		return fmt.Errorf("failed to get fix flag: %w", err)
	}

	var opts []crane.Option
	if insecure {
		opts = append(opts, crane.Insecure)
	}

	result, err := imagedigest.PullAndValidate(imageName, opts...)
	if err != nil {
		log.Error().Err(err).Msg("ValidateGostImageDigest")

		if fix {
			log.Info().Msg("Fix GOST Image Digest")
			newDigest, fixErr := imagedigest.PullAnnotatePush(imageName, opts...)
			if fixErr != nil {
				log.Fatal().Err(fixErr).Msg("AddGostImageDigest")
			}
			log.Info().Msgf("GOST Image Digest: %s", newDigest)
			log.Info().Msg("Added successfully")
			return nil
		}

		return nil
	}

	log.Info().Msgf("GOST Image Digest from image: %s", result.StoredDigest)
	log.Info().Msgf("Calculated GOST Image Digest: %s", result.CalculatedDigest)
	log.Info().Msg("Validate successfully")

	return nil
}
