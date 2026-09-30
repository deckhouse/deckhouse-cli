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
	"runtime/debug"

	cosigncli "github.com/sigstore/cosign/v2/cmd/cosign/cli"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var cosignLong = templates.LongDesc(`
Built-in cosign v2 for signing and verifying container images.

Deckhouse image signature verification (admission-policy-engine) supports
cosign v2 signatures only, cosign v3 is not supported.

Run "d8 tools cosign --help" for the full list of cosign commands.

© Flant JSC 2026`)

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cosign",
		Short: "Sign and verify container images with cosign v2",
		Long:  cosignLong,
		// Flags are passed through untouched: d8 root replaces the global flag
		// normalizer of every subcommand, which would drop cosign aliases like --cert.
		DisableFlagParsing: true,
		SilenceUsage:       true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cosignCmd := cosigncli.New()
			cosignCmd.SetArgs(args)
			// d8 prints the error and takes the exit code from CosignError.ExitCode().
			cosignCmd.SilenceErrors = true
			// cosign's own version command reports the d8 module version.
			if v, _, err := cosignCmd.Find([]string{"version"}); err == nil && v != cosignCmd {
				cosignCmd.RemoveCommand(v)
			}

			cosignCmd.AddCommand(&cobra.Command{
				Use:   "version",
				Short: "Prints the cosign version",
				Run: func(cmd *cobra.Command, _ []string) {
					cmd.Println("cosign", cosignVersion())
				},
			})

			return cosignCmd.ExecuteContext(cmd.Context())
		},
	}
}

func cosignVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range bi.Deps {
			if dep.Path == "github.com/sigstore/cosign/v2" {
				return dep.Version
			}
		}
	}

	return "unknown"
}
