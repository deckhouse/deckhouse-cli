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

package mirror

import (
	"flag"
	"os"

	"github.com/google/go-containerregistry/pkg/authn"
)

// Test configuration flags
var (
	// Source registry configuration
	sourceRegistry = flag.String("source-registry",
		getEnvOrDefault("E2E_SOURCE_REGISTRY", "registry.deckhouse.ru/deckhouse/fe"),
		"Reference registry to pull from")
	sourceUser = flag.String("source-user",
		getEnvOrDefault("E2E_SOURCE_USER", ""),
		"Source registry username (alternative to license-token)")
	sourcePassword = flag.String("source-password",
		getEnvOrDefault("E2E_SOURCE_PASSWORD", ""),
		"Source registry password (alternative to license-token)")
	licenseToken = flag.String("license-token",
		getEnvOrDefault("E2E_LICENSE_TOKEN", ""),
		"License token for source registry authentication (shortcut for source-user=license-token)")

	// Target registry configuration
	targetRegistry = flag.String("target-registry",
		getEnvOrDefault("E2E_TARGET_REGISTRY", ""),
		"Target registry to push to (empty = use in-memory registry)")
	targetUser = flag.String("target-user",
		getEnvOrDefault("E2E_TARGET_USER", ""),
		"Target registry username")
	targetPassword = flag.String("target-password",
		getEnvOrDefault("E2E_TARGET_PASSWORD", ""),
		"Target registry password")

	// Test options
	tlsSkipVerify = flag.Bool("tls-skip-verify",
		getEnvOrDefault("E2E_TLS_SKIP_VERIFY", "") == "true",
		"Skip TLS certificate verification (for self-signed certs)")
	keepBundle = flag.Bool("keep-bundle",
		getEnvOrDefault("E2E_KEEP_BUNDLE", "") == "true",
		"Keep bundle directory after test")
	d8Binary = flag.String("d8-binary",
		getEnvOrDefault("E2E_D8_BINARY", "../../../bin/d8"),
		"Path to d8 binary")

	// Debug/test options
	noModules = flag.Bool("no-modules",
		getEnvOrDefault("E2E_NO_MODULES", "") == "true",
		"Skip modules during pull (for testing failure scenarios)")
)

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Config holds the parsed test configuration
type Config struct {
	SourceRegistry string
	SourceUser     string
	SourcePassword string
	LicenseToken   string

	TargetRegistry string
	TargetUser     string
	TargetPassword string

	TLSSkipVerify bool
	KeepBundle    bool
	D8Binary      string

	// Debug/test options
	NoModules bool // Skip modules during pull (for testing failure scenarios)
}

// GetConfig returns the current test configuration from flags
func GetConfig() *Config {
	// flag.Parse() is called automatically by go test
	return &Config{
		SourceRegistry: *sourceRegistry,
		SourceUser:     *sourceUser,
		SourcePassword: *sourcePassword,
		LicenseToken:   *licenseToken,
		TargetRegistry: *targetRegistry,
		TargetUser:     *targetUser,
		TargetPassword: *targetPassword,
		TLSSkipVerify:  *tlsSkipVerify,
		KeepBundle:     *keepBundle,
		D8Binary:       *d8Binary,
		NoModules:      *noModules,
	}
}

// GetSourceAuth returns authenticator for source registry
func (c *Config) GetSourceAuth() authn.Authenticator {
	// Explicit credentials take precedence
	if c.SourceUser != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: c.SourceUser,
			Password: c.SourcePassword,
		})
	}
	// License token is a shortcut for source-user=license-token
	if c.LicenseToken != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: c.LicenseToken,
		})
	}
	return authn.Anonymous
}

// HasSourceAuth returns true if any source authentication is configured
func (c *Config) HasSourceAuth() bool {
	return c.SourceUser != "" || c.LicenseToken != ""
}

// GetTargetAuth returns authenticator for target registry
func (c *Config) GetTargetAuth() authn.Authenticator {
	if c.TargetUser != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: c.TargetUser,
			Password: c.TargetPassword,
		})
	}
	return authn.Anonymous
}

// UseInMemoryRegistry returns true if we should use in-memory registry
func (c *Config) UseInMemoryRegistry() bool {
	return c.TargetRegistry == ""
}
