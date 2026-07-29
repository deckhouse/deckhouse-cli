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

// LEGACY --source bypass (temporary).
//
// The plugin machinery reaches the registry exclusively through the in-cluster
// registry-packages-proxy (ADR #386). This file re-adds the pre-#386 direct
// registry access as a hidden escape hatch for environments without a cluster
// (CI publishing plugins straight from a registry).
//
// Everything gated behind these flags is temporary. To remove the bypass:
//   - delete this file and internal/plugins/source_legacy.go
//   - drop the AddLegacySourceFlags call in internal/plugins/cmd/plugins.go
//   - drop the SourceRegistryRepo branch in internal/plugins/init.go
// Grep marker: "legacy --source".

package flags

import (
	"os"

	"github.com/spf13/pflag"
)

// Legacy direct-registry parameters. All empty by default: an empty
// SourceRegistryRepo keeps RPP as the source, so the bypass is off unless
// --source is passed.
var (
	SourceRegistryRepo     string
	SourceRegistryLogin    string
	SourceRegistryPassword string
	DeckhouseLicenseToken  string

	Insecure      bool
	TLSSkipVerify bool
)

// AddLegacySourceFlags registers the hidden --source bypass flags on flagSet.
// They are hidden: the supported path is RPP, this is a temporary escape hatch.
func AddLegacySourceFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&SourceRegistryRepo,
		"source",
		"",
		"Pull plugins directly from this registry repo instead of the registry-packages-proxy (bypasses the cluster).",
	)
	flagSet.StringVar(
		&SourceRegistryLogin,
		"source-login",
		os.Getenv("D8_MIRROR_SOURCE_LOGIN"),
		"Source registry login. Defaults to $D8_MIRROR_SOURCE_LOGIN.",
	)
	flagSet.StringVar(
		&SourceRegistryPassword,
		"source-password",
		os.Getenv("D8_MIRROR_SOURCE_PASSWORD"),
		"Source registry password. Defaults to $D8_MIRROR_SOURCE_PASSWORD.",
	)
	flagSet.StringVar(
		&DeckhouseLicenseToken,
		"license",
		os.Getenv("D8_MIRROR_LICENSE_TOKEN"),
		"Deckhouse license key. Shortcut for --source-login=license-token --source-password=<key>. Defaults to $D8_MIRROR_LICENSE_TOKEN.",
	)
	flagSet.BoolVar(
		&TLSSkipVerify,
		"tls-skip-verify",
		false,
		"Disable TLS certificate validation for the source registry.",
	)
	flagSet.BoolVar(
		&Insecure,
		"insecure",
		false,
		"Interact with the source registry over HTTP.",
	)

	for _, name := range []string{"source", "source-login", "source-password", "license", "tls-skip-verify", "insecure"} {
		_ = flagSet.MarkHidden(name)
	}
}
