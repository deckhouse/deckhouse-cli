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
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/tools/farconverter"
)

var convertLong = templates.LongDesc(`
Converts files with Falco rules to FalcoAuditRules CRD format.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	convertCmd := &cobra.Command{
		Use:   "far-convert",
		Short: "Converts files with Falco rules to FalcoAuditRules CRD format",
		Long:  convertLong,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("this command requires exactly 1 argument, got %d", len(args))
			}

			s, err := os.Stat(args[0])
			if err != nil {
				return fmt.Errorf("Invalid path to rules file: %w", err)
			}

			if !s.Mode().IsRegular() {
				return fmt.Errorf("Argument must point to the rules YAML file")
			}

			return nil
		},
		RunE: farconverter.Convert,
	}

	return convertCmd
}
