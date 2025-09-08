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

package flags

import (
	"fmt"
	"github.com/spf13/cobra"
)

func ValidateParameters(cmd *cobra.Command, args []string) error {
	var allowedOutput = map[string]bool{
		"json": true,
		"yaml": true,
		"text": true,
	}
	outputFormat, _ := cmd.Flags().GetString("output")
	if _, valid := allowedOutput[outputFormat]; !valid {
		return fmt.Errorf("Please provide valid output: text, yaml, json. Got '%s', try --help\n", outputFormat)
	}

	watch, _ := cmd.Flags().GetBool("watch")
	if watch && outputFormat != "text" {
		return fmt.Errorf("Watch mode is only supported with text output format. Current format: %s", outputFormat)
	}

	return nil
}
