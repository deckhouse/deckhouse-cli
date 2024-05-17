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

package pull

import (
	"errors"
	"regexp"

	"github.com/spf13/cobra"
)

func parseAndValidateParameters(_ *cobra.Command, _ []string) error {
	if err := validateModuleFilterFormat(); err != nil {
		return err
	}

	return nil
}

func validateModuleFilterFormat() error {
	if ModulesFilter == "" {
		return nil
	}

	if !regexp.MustCompile(`([a-zA-Z0-9-_]+:(v\d+\.\d+\.\d+|[a-zA-Z0-9_\-]+));?`).MatchString(ModulesFilter) {
		return errors.New("Invalid filter pattern")
	}

	return nil
}
