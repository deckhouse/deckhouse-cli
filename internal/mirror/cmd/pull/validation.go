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

package pull

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/errdetect"
	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
)

func parseAndValidateParameters(_ *cobra.Command, args []string) error {
	var err error
	if err = validateSourceRegistry(); err != nil {
		return err
	}

	if err = parseAndValidateVersionFlags(); err != nil {
		return err
	}

	resolveModuleFlags()

	if err = validateProxyRegistryFlag(); err != nil {
		return err
	}

	if err = validateImagesBundlePathArg(args); err != nil {
		return err
	}

	if err = validateTmpPath(args); err != nil {
		return err
	}

	if err = validateChunkSizeFlag(); err != nil {
		return err
	}

	return nil
}

func validateSourceRegistry() error {
	if pullflags.SourceRegistryRepo == pullflags.EnterpriseEditionRepo {
		return nil // Default is fine
	}

	source := pullflags.SourceRegistryRepo

	// Check the "host:port" format of the user-provided repository URL.
	// It must have a path after the host and port.
	if _, repoPath, _ := strings.Cut(source, "/"); repoPath == "" {
		return fmt.Errorf(
			"--source %q is missing the repository path: expected format registry-host[:port]/path, e.g. %q",
			source, strings.TrimRight(source, "/")+"/deckhouse/ee",
		)
	}

	// We first validate that passed repository reference is correct and can be parsed
	if _, err := name.NewRepository(source); err != nil {
		return fmt.Errorf("--source %q is not a valid registry address: %w", source, err)
	}

	// Then we parse it as URL to validate that it contains everything we need
	registryURL, err := url.ParseRequestURI("docker://" + source)
	if err != nil {
		return fmt.Errorf("Parse --source registry address %q: %w", source, err)
	}

	if registryURL.Host == "" {
		return errors.New("--source you provided contains no registry host. Please specify source registry host address correctly")
	}

	if registryURL.Path == "" {
		return errors.New("--source you provided contains no registry path. Please specify source registry repo path correctly")
	}

	return nil
}

func validateImagesBundlePathArg(args []string) error {
	if len(args) != 1 {
		return errors.New("This command requires exactly 1 argument")
	}

	pullflags.ImagesBundlePath = filepath.Clean(args[0])

	pathInfo, err := os.Stat(pullflags.ImagesBundlePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err = os.MkdirAll(pullflags.ImagesBundlePath, 0755); err != nil {
			return fmt.Errorf("Create bundle directory at %s: %w", pullflags.ImagesBundlePath, err)
		}

		return validateImagesBundlePathArg(args)
	case err != nil:
		return err
	}

	if !pathInfo.IsDir() {
		return fmt.Errorf("%s is not a directory", pullflags.ImagesBundlePath)
	}

	if pullflags.ForcePull {
		return nil
	}

	dirEntries, err := os.ReadDir(pullflags.ImagesBundlePath)
	if err != nil {
		return fmt.Errorf("Read bundle directory: %w", err)
	}

	if len(dirEntries) == 0 || (len(dirEntries) == 1 && dirEntries[0].Name() == ".tmp" && dirEntries[0].IsDir()) {
		return nil
	}

	return fmt.Errorf("%s is not empty, use --force to override", pullflags.ImagesBundlePath)
}

func parseAndValidateVersionFlags() error {
	if pullflags.SinceVersionString != "" && pullflags.DeckhouseTag != "" {
		return errors.New("Using both --deckhouse-tag and --since-version at the same time is ambiguous")
	}

	if pullflags.PlatformConstraintString != "" && pullflags.DeckhouseTag != "" {
		return errors.New("Using both --deckhouse-tag and --include-platform at the same time is ambiguous")
	}

	if pullflags.PlatformConstraintString != "" && pullflags.SinceVersionString != "" {
		return errors.New("Using both --since-version and --include-platform at the same time is ambiguous: --include-platform already expresses a lower bound (e.g. \">=1.64\")")
	}

	var err error
	if pullflags.SinceVersionString != "" {
		pullflags.SinceVersion, err = semver.NewVersion(pullflags.SinceVersionString)
		if err != nil {
			return fmt.Errorf("Parse minimal deckhouse version: %w", err)
		}
	}

	if pullflags.PlatformConstraintString != "" {
		pullflags.PlatformConstraint, err = modules.ParseVersionConstraint(pullflags.PlatformConstraintString)
		if err != nil {
			if diag := errdetect.DiagnoseConstraintParseError(err, "include-platform", pullflags.PlatformConstraintString); diag != nil {
				return diag
			}
			return fmt.Errorf("Parse --include-platform constraint: %w", err)
		}
	}

	return nil
}

// resolveModuleFlags settles the contradiction between --no-modules and
// --include-module. A whitelist means the user wants those modules, so it
// wins: --no-modules is dropped and only the listed modules are mirrored.
func resolveModuleFlags() {
	if pullflags.NoModules && len(pullflags.ModulesWhitelist) > 0 {
		pullflags.NoModules = false

		fmt.Fprintln(os.Stderr, "Warning: --no-modules is ignored because --include-module is set; mirroring only the whitelisted modules.")
	}
}

// validateProxyRegistryFlag enforces the combinations the proxy-registry
// probe needs to work: each component that is actually being pulled (i.e.
// not switched off via --no-platform / --no-modules / --only-extra-images)
// must come with an explicit lower bound, because the probe cannot rely
// on the registry's tag catalog and has to be told where to start
// incrementing from.
//
// Notes:
//   - --deckhouse-tag / --since-version conflicts: --deckhouse-tag asks
//     for one tag (a tag-existence check is enough, no probe needed) and
//     --since-version asks for "everything from X upward without an upper
//     bound", which a probe cannot terminate safely. Both should use the
//     non-proxy path instead.
//   - --exclude-module is allowed but only meaningful when at least one
//     --include-module remains, because the probe still needs concrete
//     module names to walk.
func validateProxyRegistryFlag() error {
	if !pullflags.ProxyRegistry {
		return nil
	}

	if pullflags.DeckhouseTag != "" {
		return errors.New("--proxy-registry cannot be combined with --deckhouse-tag: pulling a single tag does not need a list-based discovery and uses the direct check-tag-exists path already")
	}

	if pullflags.SinceVersionString != "" {
		return errors.New("--proxy-registry cannot be combined with --since-version: --since-version has no upper bound, so the probe cannot terminate. Use --include-platform with an explicit lower bound (and optional upper bound) instead")
	}

	needPlatform := !pullflags.NoPlatform
	needModules := !pullflags.NoModules || pullflags.OnlyExtraImages

	// At least one component must actually be pulled — otherwise the
	// flag is a no-op against a registry that probably already failed
	// to satisfy the user.
	if !needPlatform && !needModules {
		return errors.New("--proxy-registry has nothing to do: both --no-platform and --no-modules are set")
	}

	if needPlatform && pullflags.PlatformConstraintString == "" {
		return errors.New("--proxy-registry requires --include-platform (or --no-platform to skip platform mirroring): the probe needs an explicit lower bound to start incrementing from")
	}

	if needModules {
		if len(pullflags.ModulesWhitelist) == 0 {
			return errors.New("--proxy-registry requires --include-module (or --no-modules to skip module mirroring): the probe needs explicit module names and version anchors to start incrementing from")
		}
		// Every --include-module entry must come with an explicit
		// version part. The implicit ">=0.0.0" fallback used by the
		// regular pull mode is poisonous for the probe: it starts at
		// v0.0.0 and stops on the first not-found, silently skipping
		// any module whose lowest tag is above v0.0.0 / v0.1.0 /
		// v1.0.0. Bail out loudly so the user picks a real anchor.
		for _, entry := range pullflags.ModulesWhitelist {
			if !strings.Contains(entry, "@") {
				return fmt.Errorf("--proxy-registry requires every --include-module entry to specify an explicit version constraint (e.g. %q@^1.0.0); without it the probe would start at v0.0.0 and miss everything", strings.TrimSpace(entry))
			}
		}
	}

	return nil
}

func validateChunkSizeFlag() error {
	if pullflags.ImagesBundleChunkSizeGB < 0 {
		return errors.New("Chunk size cannot be less than zero GB")
	}

	return nil
}

func validateTmpPath(_ []string) error {
	if pullflags.TempDir == "" {
		pullflags.TempDir = filepath.Join(pullflags.ImagesBundlePath, ".tmp")
	}

	if err := os.MkdirAll(pullflags.TempDir, 0755); err != nil {
		return fmt.Errorf("Error creating temp directory at %s: %w", pullflags.TempDir, err)
	}

	return nil
}
