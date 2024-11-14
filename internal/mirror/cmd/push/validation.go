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

package push

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

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
	bundleExtension := filepath.Ext(ImagesBundlePath)
	stat, err := os.Stat(ImagesBundlePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			stat, err = os.Stat(ImagesBundlePath + ".0000.chunk")
			if err != nil {
				return fmt.Errorf("invalid images bundle path: %w", err)
			}
			return nil
		}
		return fmt.Errorf("invalid images bundle path: %w", err)
	}

	switch {
	case bundleExtension != ".tar" && !stat.IsDir():
		return errors.New("images-bundle-path argument should be a path to tar archive (.tar) or a directory containing unpacked bundle")
	case bundleExtension == "" && !stat.IsDir():
		return fmt.Errorf("%s: not a directory", ImagesBundlePath)
	default:
		return nil
	}
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

	registryUrl, err := url.ParseRequestURI("docker://" + registry)
	if err != nil {
		return fmt.Errorf("Validate registry address: %w", err)
	}
	RegistryHost = registryUrl.Host
	RegistryPath = registryUrl.Path
	if RegistryHost == "" {
		return errors.New("--registry you provided contains no registry host. Please specify registry address correctly.")
	}
	if RegistryPath == "" {
		return errors.New("--registry you provided contains no path to repo. Please specify registry repo path correctly.")
	}

	return nil
}
