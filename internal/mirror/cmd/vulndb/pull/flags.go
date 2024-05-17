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

package pull

import (
	"os"

	"github.com/spf13/pflag"
)

func addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&SourceRegistryRepo,
		"source",
		enterpriseEditionRepo,
		"Pull Deckhouse images from source registry. This is the default mode of operation.",
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
		&LicenseToken,
		"license",
		"l",
		os.Getenv("D8_MIRROR_LICENSE_TOKEN"),
		"Deckhouse license key.",
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
}
