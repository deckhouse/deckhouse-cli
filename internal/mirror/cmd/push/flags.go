// Copyright 2024 Flant JSC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package push

import (
	"github.com/spf13/pflag"
)

func addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&SourceRegistryRepo,
		"source",
		enterpriseEditionRepo,
		"Source repository from which Deckhouse images were pulled.",
	)
	flagSet.StringVarP(
		&registry,
		"registry",
		"r",
		"",
		"Push Deckhouse images to your private registry, specified as registry-host[:port]/path. Conflicts with --license.",
	)
	flagSet.StringVarP(
		&RegistryUsername,
		"registry-login",
		"u",
		"",
		"Username to log into your registry.",
	)
	flagSet.StringVarP(
		&RegistryPassword,
		"registry-password",
		"p",
		"",
		"Password to log into your registry.",
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
