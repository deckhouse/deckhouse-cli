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

package push

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

func parseAndValidateParameters(_ *cobra.Command, _ []string) error {
	if err := validateRegistryFlags(); err != nil {
		return err
	}

	return nil
}

func validateRegistryFlags() error {
	if MirrorModulesRegistry != "" {
		_, err := url.Parse("docker://" + MirrorModulesRegistry)
		if err != nil {
			return fmt.Errorf("Malformed registry URL: %w", err)
		}
	}

	if MirrorModulesRegistryPassword != "" && MirrorModulesRegistryUsername == "" {
		return errors.New("Registry credentials not provided")
	}
	return nil
}
