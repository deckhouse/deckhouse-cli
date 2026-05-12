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

// Package cr wires the `d8 cr` subtree. The root command lives here, the
// subcommands live in the basic/ and fs/ subpackages. All of them share a
// single *registry.Options populated at PersistentPreRunE time.
package cr

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/basic"
	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/fs"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

const (
	cmdShort = "Work with container registries"

	cmdLong = `Work with container images in OCI/Docker registries: inspect metadata,
transfer (pull/push), and browse contents.

Authentication uses the Docker config (~/.docker/config.json) - run
"d8 login" first if the registry requires credentials.
`
)

// NewCommand returns the `d8 cr` cobra subtree.
func NewCommand() *cobra.Command {
	opts := registry.New()

	cr := &cobra.Command{
		Use:           "cr",
		Short:         cmdShort,
		Long:          cmdLong,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	setupRootFlags(cr, opts)
	cr.AddCommand(
		basic.NewPullCmd(opts),
		basic.NewPushCmd(opts),
		basic.NewExportCmd(opts),
		basic.NewLsCmd(opts),
		basic.NewCatalogCmd(opts),
		basic.NewManifestCmd(opts),
		basic.NewConfigCmd(opts),
		basic.NewDigestCmd(opts),
		fs.NewCommand(opts),
	)

	return cr
}
