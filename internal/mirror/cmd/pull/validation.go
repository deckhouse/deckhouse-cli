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

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/spf13/cobra"
)

func parseAndValidateParameters(_ *cobra.Command, args []string) error {
	var err error
	if err = validateSourceRegistry(); err != nil {
		return err
	}
	if err = parseAndValidateVersionFlags(); err != nil {
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
	if SourceRegistryRepo == enterpriseEditionRepo {
		return nil // Default is fine
	}

	// We first validate that passed repository reference is correct and can be parsed
	if _, err := name.NewRepository(SourceRegistryRepo); err != nil {
		return fmt.Errorf("Validate registry address: %w", err)
	}

	// Then we parse it as URL to validate that it contains everything we need
	registryURL, err := url.ParseRequestURI("docker://" + SourceRegistryRepo)
	if err != nil {
		return fmt.Errorf("Validate source registry parameter: %w", err)
	}
	if registryURL.Host == "" {
		return errors.New("--source you provided contains no registry host. Please specify source registry host address correctly.")
	}
	if registryURL.Path == "" {
		return errors.New("--source you provided contains no registry path. Please specify source registry repo path correctly.")
	}

	return nil
}

func validateImagesBundlePathArg(args []string) error {
	if len(args) != 1 {
		return errors.New("This command requires exactly 1 argument")
	}

	ImagesBundlePath = filepath.Clean(args[0])
	pathInfo, err := os.Stat(ImagesBundlePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err = os.MkdirAll(ImagesBundlePath, 0755); err != nil {
			return fmt.Errorf("Create bundle directory at %s: %w", ImagesBundlePath, err)
		}
		return validateImagesBundlePathArg(args)
	case err != nil:
		return err
	}

	if !pathInfo.IsDir() {
		return fmt.Errorf("%s is not a directory", ImagesBundlePath)
	}

	if ForcePull {
		return nil
	}

	dirEntries, err := os.ReadDir(ImagesBundlePath)
	if err != nil {
		return fmt.Errorf("Read bundle directory: %w", err)
	}

	if len(dirEntries) == 0 || (len(dirEntries) == 1 && dirEntries[0].Name() == ".tmp" && dirEntries[0].IsDir()) {
		return nil
	}

	return fmt.Errorf("%s is not empty, use --force to override", ImagesBundlePath)
}

func parseAndValidateVersionFlags() error {
	if sinceVersionString != "" && DeckhouseTag != "" {
		return errors.New("Using both --deckhouse-tag and --since-version at the same time is ambiguous.")
	}

	var err error
	if sinceVersionString != "" {
		SinceVersion, err = semver.NewVersion(sinceVersionString)
		if err != nil {
			return fmt.Errorf("Parse minimal deckhouse version: %w", err)
		}
	}

	return nil
}

func validateChunkSizeFlag() error {
	if ImagesBundleChunkSizeGB < 0 {
		return errors.New("Chunk size cannot be less than zero GB")
	}

	return nil
}

func validateTmpPath(_ []string) error {
	if TempDir == "" {
		TempDir = filepath.Join(ImagesBundlePath, ".tmp", "mirror")
	}
	if err := os.MkdirAll(TempDir, 0755); err != nil {
		return fmt.Errorf("Error creating temp directory at %s: %w", TempDir, err)
	}
	return nil
}
