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

package internal

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
)

const (
	SecurityTestTimeout = 30 * time.Minute
	PlatformTestTimeout = 2 * time.Hour
	ModulesTestTimeout  = 2 * time.Hour
	FullCycleTestTimeout = 3 * time.Hour
)

var (
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

	targetRegistry = flag.String("target-registry",
		getEnvOrDefault("E2E_TARGET_REGISTRY", ""),
		"Target registry to push to (empty = use in-memory registry)")
	targetUser = flag.String("target-user",
		getEnvOrDefault("E2E_TARGET_USER", ""),
		"Target registry username")
	targetPassword = flag.String("target-password",
		getEnvOrDefault("E2E_TARGET_PASSWORD", ""),
		"Target registry password")

	tlsSkipVerify = flag.Bool("tls-skip-verify",
		getEnvOrDefault("E2E_TLS_SKIP_VERIFY", "") == "true",
		"Skip TLS certificate verification (for self-signed certs)")
	keepBundle = flag.Bool("keep-bundle",
		getEnvOrDefault("E2E_KEEP_BUNDLE", "") == "true",
		"Keep bundle directory after test")
	existingBundle = flag.String("existing-bundle",
		getEnvOrDefault("E2E_EXISTING_BUNDLE", ""),
		"Path to existing bundle directory (skip pull step)")
	d8Binary = flag.String("d8-binary",
		getEnvOrDefault("E2E_D8_BINARY", "bin/d8"),
		"Path to d8 binary")

	deckhouseTag = flag.String("deckhouse-tag",
		getEnvOrDefault("E2E_DECKHOUSE_TAG", ""),
		"Specific Deckhouse tag or release channel (e.g., 'stable', 'v1.65.8')")
	noModules = flag.Bool("no-modules",
		getEnvOrDefault("E2E_NO_MODULES", "") == "true",
		"Skip modules during pull")
	noPlatform = flag.Bool("no-platform",
		getEnvOrDefault("E2E_NO_PLATFORM", "") == "true",
		"Skip platform during pull")
	noSecurity = flag.Bool("no-security",
		getEnvOrDefault("E2E_NO_SECURITY", "") == "true",
		"Skip security databases during pull")
	includeModules = flag.String("include-modules",
		getEnvOrDefault("E2E_INCLUDE_MODULES", ""),
		"Comma-separated list of modules to include (empty = all)")

	newPull = flag.Bool("new-pull",
		getEnvOrDefault("E2E_NEW_PULL", "") == "true",
		"Use new pull implementation")
)

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

type Config struct {
	SourceRegistry string
	SourceUser     string
	SourcePassword string
	LicenseToken   string

	TargetRegistry string
	TargetUser     string
	TargetPassword string

	TLSSkipVerify  bool
	KeepBundle     bool
	ExistingBundle string
	D8Binary       string

	DeckhouseTag   string
	NoModules      bool
	NoPlatform      bool
	NoSecurity      bool
	IncludeModules  []string

	NewPull bool
}

func GetConfig() *Config {
	cfg := &Config{
		SourceRegistry: *sourceRegistry,
		SourceUser:     *sourceUser,
		SourcePassword: *sourcePassword,
		LicenseToken:   *licenseToken,
		TargetRegistry: *targetRegistry,
		TargetUser:     *targetUser,
		TargetPassword: *targetPassword,
		TLSSkipVerify:  *tlsSkipVerify,
		KeepBundle:     *keepBundle,
		ExistingBundle: *existingBundle,
		D8Binary:       resolveD8Binary(*d8Binary),
		DeckhouseTag:   *deckhouseTag,
		NoModules:      *noModules,
		NoPlatform:     *noPlatform,
		NoSecurity:     *noSecurity,
		NewPull:        *newPull,
	}

	if *includeModules != "" {
		cfg.IncludeModules = parseCommaSeparated(*includeModules)
	}

	return cfg
}

func resolveD8Binary(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	projectRoot := FindProjectRoot()
	return filepath.Join(projectRoot, path)
}

func FindProjectRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			dir, _ = os.Getwd()
			return dir
		}
		dir = parent
	}
}



func parseCommaSeparated(s string) []string {
	if s == "" {
		return nil
	}
	var parts []string
	for _, part := range strings.Split(s, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func (c *Config) GetSourceAuth() authn.Authenticator {
	if c.SourceUser != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: c.SourceUser,
			Password: c.SourcePassword,
		})
	}
	if c.LicenseToken != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: c.LicenseToken,
		})
	}
	return authn.Anonymous
}

func (c *Config) HasSourceAuth() bool {
	return c.SourceUser != "" || c.LicenseToken != ""
}

func (c *Config) GetTargetAuth() authn.Authenticator {
	if c.TargetUser != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: c.TargetUser,
			Password: c.TargetPassword,
		})
	}
	return authn.Anonymous
}

func (c *Config) UseInMemoryRegistry() bool {
	return c.TargetRegistry == ""
}

func (c *Config) HasExistingBundle() bool {
	return c.ExistingBundle != ""
}

