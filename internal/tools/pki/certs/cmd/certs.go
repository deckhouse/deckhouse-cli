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
)

var certsLong = templates.LongDesc(`
Manage and inspect control-plane TLS certificates.

© Flant JSC 2026`)

// NewCommand returns the "pki certs" group command.
func NewCommand() *cobra.Command {
	certsCmd := &cobra.Command{
		Use:   "certs",
		Short: "Manage and inspect control-plane TLS certificates",
		Long:  certsLong,
	}

	certsCmd.AddCommand(NewCheckCommand())
	certsCmd.AddCommand(NewRenewCommand())

	return certsCmd
}
