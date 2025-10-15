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
)

func parseAndValidateParameters(_ *cobra.Command, args []string) error {
	if len(args) != 2 {
		return errors.New("invalid number of arguments, expected 2")
	}

	var err error
	if err = parseAndValidateRegistryURLArg(args); err != nil {
		return err
	}
	if err = validateRegistryCredentials(); err != nil {
		return err
	}
	if err = validateImagesBundlePathArg(args); err != nil {
		return err
	}

	return nil
}

func validateImagesBundlePathArg(args []string) error {
	ImagesBundlePath = filepath.Clean(args[0])
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
			ext := filepath.Ext(item.Name())
			return ext == ".tar" || ext == ".chunk"
		})
		if len(dirEntries) == 0 {
			return errors.New("no packages found in bundle directory")
		}

		if TempDir == "" {
			TempDir = filepath.Join(ImagesBundlePath, ".tmp")
		}

		return nil
	}

	if bundleExtension := filepath.Ext(ImagesBundlePath); bundleExtension == ".tar" || bundleExtension == ".chunk" {
		if TempDir == "" {
			TempDir = filepath.Join(filepath.Dir(ImagesBundlePath), ".tmp")
		}
		return nil
	}

	return fmt.Errorf("invalid images bundle: must be a directory, tar or a chunked package")
}

func validateRegistryCredentials() error {
	if RegistryPassword != "" && RegistryUsername == "" {
		return errors.New("registry username not specified")
	}
	return nil
}

func parseAndValidateRegistryURLArg(args []string) error {
	registry := strings.NewReplacer("http://", "", "https://", "").Replace(args[1])
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
		return errors.New("<registry> you provided contains no registry host. Please specify registry address correctly.")
	}
	if len(RegistryPath) < 2 || len(RegistryPath) > 255 {
		return errors.New("repository part must be between 2 and 255 characters in length. Please specify registry repo path correctly.")
	}

	return nil
}
