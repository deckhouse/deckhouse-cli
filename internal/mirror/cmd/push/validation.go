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

package push

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/samber/lo"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
)

func parseAndValidateParameters(_ *cobra.Command, args []string) error {
	// The registry is always the last argument. The bundle path is an optional
	// first argument; when it is omitted, packages must be provided via --file.
	var (
		registryArg string
		bundleArg   []string
	)

	switch len(args) {
	case 1:
		registryArg = args[0]
	case 2:
		bundleArg = args[:1]
		registryArg = args[1]
	default:
		return errors.New("invalid number of arguments, expected <registry> with an optional bundle path before it")
	}

	var err error
	if err = parseAndValidateRegistryURLArg(registryArg); err != nil {
		return err
	}

	if err = validateRegistryCredentials(); err != nil {
		return err
	}

	if err = resolvePackages(bundleArg); err != nil {
		return err
	}

	return nil
}

// resolvePackages builds the list of package archives to push from the optional
// bundle path argument and the --file flag, then sets the default temp dir.
func resolvePackages(bundleArg []string) error {
	Packages = nil

	if len(bundleArg) == 1 {
		if err := collectBundlePathPackages(bundleArg[0]); err != nil {
			return err
		}
	}

	if err := collectFilesPackages(); err != nil {
		return err
	}

	if len(Packages) == 0 {
		return errors.New("no packages to push: specify a bundle directory before registry URL, or use --file to specify tar/chunked package")
	}

	Packages = lo.Uniq(Packages)

	if TempDir == "" {
		TempDir = filepath.Join(filepath.Dir(Packages[0]), ".tmp", mirror.TmpMirrorFolderName)
	}

	return nil
}

// collectBundlePathPackages resolves the bundle path argument, which may be a
// directory of packages or a single tar/chunked package, into Packages.
func collectBundlePathPackages(arg string) error {
	ImagesBundlePath = filepath.Clean(arg)

	s, err := os.Stat(ImagesBundlePath)
	if err != nil {
		return fmt.Errorf("could not read images bundle: %w", err)
	}

	if s.IsDir() {
		dirEntries, err := os.ReadDir(ImagesBundlePath)
		if err != nil {
			return fmt.Errorf("could not list files in bundle directory: %w", err)
		}

		dirEntries = lo.Filter(dirEntries, func(item os.DirEntry, _ int) bool {
			return isPackageFile(item.Name())
		})
		if len(dirEntries) == 0 {
			return errors.New("no packages found in bundle directory")
		}

		for _, entry := range dirEntries {
			// Chunk files (<name>.tar.NNNN.chunk) collapse to a single <name>.tar
			// package; the pusher reassembles the chunks at push time.
			Packages = append(Packages, canonicalPackagePath(filepath.Join(ImagesBundlePath, entry.Name())))
		}

		// Default temp dir lives inside the bundle directory, as before.
		if TempDir == "" {
			TempDir = filepath.Join(ImagesBundlePath, ".tmp", mirror.TmpMirrorFolderName)
		}

		return nil
	}

	if isPackageFile(ImagesBundlePath) {
		Packages = append(Packages, canonicalPackagePath(ImagesBundlePath))
		return nil
	}

	return fmt.Errorf("invalid images bundle: must be a directory, tar or a chunked package")
}

// collectFilesPackages validates and appends the packages passed via --file.
func collectFilesPackages() error {
	for _, f := range Files {
		path := filepath.Clean(f)

		s, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("could not read package %q: %w", path, err)
		}

		if s.IsDir() {
			return fmt.Errorf("--file entry %q is a directory, expected a tar or chunked package", path)
		}

		if !isPackageFile(path) {
			return fmt.Errorf("--file entry %q is not a tar or chunked package", path)
		}

		Packages = append(Packages, canonicalPackagePath(path))
	}

	return nil
}

func isPackageFile(name string) bool {
	ext := filepath.Ext(name)
	return ext == ".tar" || ext == ".chunk"
}

// canonicalPackagePath maps a chunk file (<name>.tar.NNNN.chunk) to its canonical
// <name>.tar path so the pusher reassembles all chunks instead of reading a single
// chunk as a whole archive. Plain .tar paths are returned unchanged.
func canonicalPackagePath(path string) string {
	if filepath.Ext(path) != ".chunk" {
		return path
	}

	if idx := strings.Index(path, ".tar."); idx != -1 {
		return path[:idx] + ".tar"
	}

	return path
}

func validateRegistryCredentials() error {
	if RegistryPassword != "" && RegistryUsername == "" {
		return errors.New("registry username not specified")
	}

	return nil
}

func parseAndValidateRegistryURLArg(registryArg string) error {
	registry := strings.NewReplacer("http://", "", "https://", "").Replace(registryArg)
	if registry == "" {
		return errors.New("<registry> argument is empty")
	}

	// We first validate that passed repository reference is correct and can be parsed
	if _, err := name.NewRepository(registry); err != nil {
		return fmt.Errorf("Validate registry address: %w", err)
	}

	// Then we parse it as URL to validate that it contains everything we need
	registryURL, err := url.ParseRequestURI("docker://" + registry)
	if err != nil {
		return fmt.Errorf("Validate registry address: %w", err)
	}

	RegistryHost = registryURL.Host
	RegistryPath = registryURL.Path

	if RegistryHost == "" {
		return errors.New("<registry> you provided contains no registry host. Please specify registry address correctly")
	}

	if len(RegistryPath) < 2 || len(RegistryPath) > 255 {
		return errors.New("repository part must be between 2 and 255 characters in length. Please specify registry repo path correctly")
	}

	return nil
}
