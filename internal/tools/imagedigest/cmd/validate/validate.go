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
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest"
)

func NewCommand() *cobra.Command {
	validateCmd := &cobra.Command{
		Use:           "validate <image>",
		Short:         "Validating the image digest in the image metadata calculated according to the GOST standard Streebog (GOST R 34.11-2012)",
		Long:          `Validating the image digest in the image metadata calculated according to the GOST standard Streebog (GOST R 34.11-2012)`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("this command requires exactly 1 argument (image reference), got %d", len(args))
			}
			return nil
		},
		RunE: runValidate,
	}

	validateCmd.Flags().Bool("fix", false, "Automatically fix GOST digest if validation fails")

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

	fmt.Printf("Validating GOST digest for image: %s\n", imageName)

	result, err := imagedigest.PullAndValidate(imageName, opts...)
	if err != nil {
		if result != nil {
			fmt.Printf("Stored GOST digest:     %s\n", result.StoredDigest)
			fmt.Printf("Calculated GOST digest: %s\n", result.CalculatedDigest)
		}
		fmt.Printf("Validation failed: %v\n", err)

		if fix {
			fmt.Println("Attempting to fix GOST digest...")
			newDigest, fixErr := imagedigest.PullAnnotatePush(imageName, opts...)
			if fixErr != nil {
				return fmt.Errorf("failed to fix GOST digest: %w", fixErr)
			}
			fmt.Printf("New GOST digest: %s\n", newDigest)
			fmt.Println("Digest fixed successfully")
			return nil
		}

		return err
	}

	fmt.Printf("GOST digest: %s\n", result.StoredDigest)
	fmt.Println("Validation successful")

	return nil
}
