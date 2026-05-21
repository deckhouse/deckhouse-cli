/*
Copyright 2026 Flant JSC

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

	certscmd "github.com/deckhouse/deckhouse-cli/internal/tools/pki/certs/cmd"
)

var pkiLong = templates.LongDesc(`
Tools for working with control-plane PKI.

© Flant JSC 2026`)

// NewCommand returns the "tools pki" group command.
func NewCommand() *cobra.Command {
	pkiCmd := &cobra.Command{
		Use:   "pki",
		Short: "Tools for working with control-plane PKI",
		Long:  pkiLong,
	}

	pkiCmd.AddCommand(certscmd.NewCommand())

	return pkiCmd
}
