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

package cr

import (
	"fmt"
	"io"
	"os"

	"github.com/google/go-containerregistry/pkg/logs"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/rootflagnames"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

// setupRootFlags installs the four persistent flags on cmd and wires a
// PersistentPreRunE that feeds them into opts. Running order matters: cobra
// invokes PersistentPreRunE on the root before any subcommand's RunE, so by
// the time RunE reads from opts the struct is fully populated.
//
// Idempotent: PersistentPreRunE resets opts to a fresh state on each call,
// so a second invocation (test harness, embedder) cannot stack duplicate
// remote/name options.
func setupRootFlags(cmd *cobra.Command, opts *registry.Options) {
	var (
		verbose  bool
		insecure bool
		ndLayers bool
		platform string
	)

	flags := cmd.PersistentFlags()
	flags.BoolVarP(&verbose, rootflagnames.Verbose, "v", false, "Enable debug logs on stderr")
	flags.BoolVar(&insecure, rootflagnames.Insecure, false, "Allow plain HTTP and skip TLS verification (localhost and RFC1918 hosts already auto-allow HTTP)")
	flags.BoolVar(&ndLayers, rootflagnames.AllowNondistributable, false, "Include non-distributable (foreign) layers when pushing")
	flags.StringVar(&platform, rootflagnames.Platform, "", "Resolve images to platform os/arch[/variant][:osversion] (image-level commands only)")
	// No completion for --platform on purpose: the set of platforms a given
	// image actually serves depends on its manifest list, which we cannot
	// know at flag-completion time (the IMAGE arg may not even be typed
	// yet). A static list of "common platforms" would mislead users into
	// thinking those values are guaranteed to work, so we leave it free-form.

	cmd.PersistentPreRunE = func(c *cobra.Command, _ []string) error {
		// Reset before applying, so a re-entry (test harness re-invoking
		// the same command, an embedder running PersistentPreRunE twice)
		// can never double-append to opts.Remote / opts.Name.
		*opts = *registry.New()
		opts.WithContext(c.Context())
		applyVerbose(verbose)
		if insecure {
			opts.WithInsecure().WithTransport(registry.InsecureTransport())
		}
		if ndLayers {
			opts.WithNondistributable()
		}
		if platform != "" {
			p, err := v1.ParsePlatform(platform)
			if err != nil {
				return fmt.Errorf("parse --platform: %w", err)
			}
			opts.WithPlatform(p)
		}
		return nil
	}
}

// applyVerbose toggles go-containerregistry's debug logger. logs.Debug is a
// package-level *log.Logger, so we must explicitly route to io.Discard when
// verbose is off - otherwise a previous "-v" run in the same process would
// keep leaking debug output.
func applyVerbose(verbose bool) {
	if verbose {
		logs.Debug.SetOutput(os.Stderr)
		return
	}
	logs.Debug.SetOutput(io.Discard)
}
