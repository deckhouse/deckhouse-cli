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

package loki

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

var lokiLong = templates.LongDesc(`
Dump Loki logs.
		
This command dump all logs from Loki api or in given range timestamps in DKP.

© Flant JSC 2025`)

var config = &Config{}

func NewCommand() *cobra.Command {
	lokiCmd := &cobra.Command{
		Use:           "loki",
		Short:         "Dump logs from Loki api.",
		Long:          lokiLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       flags.ValidateParameters,
		RunE:          runLoki,
	}
	addFlags(lokiCmd.Flags())
	return lokiCmd
}

func runLoki(cmd *cobra.Command, _ []string) error {
	runner := NewRunner(config)
	return runner.Run(cmd.Context(), cmd)
}
