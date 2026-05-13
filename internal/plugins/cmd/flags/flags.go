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

package flags

import (
	"os"

	"github.com/spf13/pflag"
)

const (
	deckhouseRegistryHost = "registry.deckhouse.io"

	EnterpriseEditionRepo = deckhouseRegistryHost + "/deckhouse/ee"

	DefaultDeckhousePluginsDir = "/opt/deckhouse/lib/deckhouse-cli"
)

// CLI Parameters
var (
	DeckhousePluginsDir = DefaultDeckhousePluginsDir

	Insecure      bool
	TLSSkipVerify bool

	SourceRegistryRepo     = EnterpriseEditionRepo // Fallback to EE if nothing was given as source.
	SourceRegistryLogin    string
	SourceRegistryPassword string
	DeckhouseLicenseToken  string
)

func AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&SourceRegistryRepo,
		"source",
		SourceRegistryRepo,
		"Source registry to pull Deckhouse plugins from.",
	)
	flagSet.StringVar(
		&SourceRegistryLogin,
		"source-login",
		os.Getenv("D8_MIRROR_SOURCE_LOGIN"),
		"Source registry login.",
	)
	flagSet.StringVar(
		&SourceRegistryPassword,
		"source-password",
		os.Getenv("D8_MIRROR_SOURCE_PASSWORD"),
		"Source registry password.",
	)
	flagSet.StringVarP(
		&DeckhouseLicenseToken,
		"license",
		"l",
		os.Getenv("D8_MIRROR_LICENSE_TOKEN"),
		"Deckhouse license key. Shortcut for --source-login=license-token --source-password=<>.",
	)
	flagSet.BoolVar(
		&TLSSkipVerify,
		"tls-skip-verify",
		false,
		"Disable TLS certificate validation.",
	)
	flagSet.BoolVar(
		&Insecure,
		"insecure",
		false,
		"Interact with registries over HTTP.",
	)
	flagSet.StringVar(
		&DeckhousePluginsDir,
		"plugins-dir",
		DeckhousePluginsDir,
		"Path to the d8 plugins directory.",
	)
}
