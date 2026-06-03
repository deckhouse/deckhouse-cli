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

// Package flags declares the CLI flags and environment defaults for reaching the
// registry-packages-proxy.
package flags

import (
	"os"

	"github.com/spf13/pflag"
)

const (
	EnvEndpoint = "D8_RPP_ENDPOINT"
	EnvCAFile   = "D8_RPP_CA_FILE"
)

// CLI parameters for the registry-packages-proxy connection.
var (
	// Endpoint is the proxy base URL (e.g. https://<master-ip>:4219, or the
	// public Ingress). Empty means discover it from the cluster.
	Endpoint = os.Getenv(EnvEndpoint)

	// CAFile points to a PEM CA bundle used to verify the proxy TLS certificate
	// in addition to the system roots.
	CAFile = os.Getenv(EnvCAFile)

	// InsecureSkipTLSVerify disables proxy TLS verification (debugging only).
	InsecureSkipTLSVerify bool
)

// AddFlags registers the registry-packages-proxy connection flags on flagSet.
func AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&Endpoint,
		"rpp-endpoint",
		Endpoint,
		"registry-packages-proxy base URL (e.g. https://master:4219). Discovered from the cluster when empty. Defaults to $"+EnvEndpoint+".",
	)
	flagSet.StringVar(
		&CAFile,
		"rpp-ca-file",
		CAFile,
		"Path to a PEM CA bundle used to verify the registry-packages-proxy TLS certificate. Defaults to $"+EnvCAFile+".",
	)
	flagSet.BoolVar(
		&InsecureSkipTLSVerify,
		"rpp-insecure-skip-tls-verify",
		false,
		"Skip registry-packages-proxy TLS verification. For debugging only.",
	)
}
