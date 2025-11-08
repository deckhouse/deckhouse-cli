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

package commands

import (
	"os"
	"strings"

	vaultcommand "github.com/hashicorp/vault/command"
	"github.com/spf13/cobra"
)

type Commands struct {
	Command     string
	Description string
}

func MapCustomEnvsToInternalEnvs(envNamePrefix string, envNameMapPrefix string) {
	env := os.Environ()
	for _, keyValue := range env {
		parts := strings.SplitN(keyValue, "=", 2)
		if strings.HasPrefix(parts[0], envNamePrefix) {
			replacedEnvName := strings.Replace(parts[0], envNamePrefix, envNameMapPrefix, 1)
			os.Setenv(replacedEnvName, parts[1])
		}
	}
}

func NewStrongholdCommand() *cobra.Command {
	MapCustomEnvsToInternalEnvs("STRONGHOLD_", "VAULT_")

	strongholdCommands := []Commands{
		{"read", "Read data and retrieves secrets"},
		{"write", "Write data, configuration, and secrets"},
		{"delete", "Delete secrets and configuration"},
		{"list", "List data or secrets"},
		{"login", "Authenticate locally"},
		{"status", "Print seal and HA status"},
		{"unwrap", "Unwrap a wrapped secret"},
		{"kv", "Interact with Stronghold's Key-Value storage"},
		{"policy", "Interact with policies"},
		{"pki", "Interact with Stronghold's PKI Secrets Engine"},
		{"operator", "Perform operator-specific tasks"},
		{"secrets", "Interact with secrets engines"},
		{"token", "Interact with tokens"},
		{"namespace", "Interact with namespaces"},
		{"lease", "Interact with leases"},
		{"transit", "Interact with Stronghold's Transit Secrets Engine"},
		{"auth", "Interact with auth methods"},
		{"print", "Prints runtime configurations"},
		{"patch", "Patch data, configuration, and secrets"},
		{"path-help", "Retrieve API help for paths"},
		{"version", "Print version"},
	}

	strongholdCmd := &cobra.Command{
		Use:           "stronghold",
		Short:         "Deckhouse Stronghold commands",
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	for _, cmd := range strongholdCommands {
		strongholdCommand := []string{cmd.Command}
		strongholdSubCmd := &cobra.Command{
			Use:                cmd.Command,
			Short:              cmd.Description,
			DisableFlagParsing: true,
			SilenceErrors:      true,
			SilenceUsage:       true,
			Run: func(_ *cobra.Command, args []string) {
				vaultcommand.Run(append(strongholdCommand, args...))
			},
		}
		strongholdCmd.AddCommand(strongholdSubCmd)
	}

	return strongholdCmd
}
