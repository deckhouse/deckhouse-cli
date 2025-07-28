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

	"github.com/deckhouse/deckhouse-cli/internal/tools/gostsum"
)

var gostsumLong = templates.LongDesc(`
Calculate Streebog checksum of the data according to the RFC 6986 (GOST R 34.11-2012) specifications.

Data for checksumming is provided as a list of filepaths in the command-line arguments.
If no filepaths are provided, data is assumed to be available from stdin.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	convertCmd := &cobra.Command{
		Use:   "gostsum",
		Short: "Calculate Streebog checksum of the data according to the RFC 6986 (GOST R 34.11-2012) specifications",
		Long:  gostsumLong,
		RunE:  gostsum.Gostsum,
	}

	addFlags(convertCmd.Flags())

	return convertCmd
}
