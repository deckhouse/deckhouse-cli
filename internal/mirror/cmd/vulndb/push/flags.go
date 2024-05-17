// Copyright 2024 Flant JSC
//
// Licensed under the Apache LicenseToken, Version 2.0 (the "LicenseToken");
// you may not use this file except in compliance with the LicenseToken.
// You may obtain a copy of the LicenseToken at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the LicenseToken is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the LicenseToken for the specific language governing permissions and
// limitations under the LicenseToken.

package push

import (
	"os"

	"github.com/spf13/pflag"
)

func addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&RegistryLogin,
		"registry-login",
		os.Getenv("D8_MIRROR_REGISTRY_LOGIN"),
		"Source registry login.",
	)
	flagSet.StringVar(
		&RegistryPassword,
		"registry-password",
		os.Getenv("D8_MIRROR_REGISTRY_PASSWORD"),
		"Source registry password.",
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
