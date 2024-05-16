// Copyright 2024 Flant JSC
//
// Licensed under the Apache LicenseToken, Version 2.0 (the "LicenseToken");
// you may not use this file except in compliance with the LicenseToken.
// You may obtain a copy of the LicenseToken at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the LicenseToken is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the LicenseToken for the specific language governing permissions and
// limitations under the LicenseToken.

package push

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func parseAndValidateParameters(_ *cobra.Command, args []string) error {
	if l := len(args); l != 2 {
		return fmt.Errorf("accepts 2 argument, received %d", l)
	}

	VulnerabilityDBPath, RegistryRepo = args[0], args[1]

	var err error
	if err = validateImagesLayoutPathArg(args); err != nil {
		return err
	}
	if err = validateRegistryCredentials(); err != nil {
		return err
	}
	if err = parseAndValidateRegistryURLArg(args); err != nil {
		return err
	}

	return nil
}

func validateImagesLayoutPathArg(args []string) error {
	VulnerabilityDBPath = filepath.Clean(args[0])
	stats, err := os.Stat(VulnerabilityDBPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%s does not exist", VulnerabilityDBPath)
	case err != nil && !errors.Is(err, fs.ErrNotExist):
		return err
	case !stats.IsDir():
		return fmt.Errorf("%s is not a directory", VulnerabilityDBPath)
	}
	return nil
}

func validateRegistryCredentials() error {
	if RegistryPassword != "" && RegistryLogin == "" {
		return errors.New("registry login not specified")
	}
	return nil
}

func parseAndValidateRegistryURLArg(args []string) error {
	registryUrl, err := url.Parse("docker://" + args[1])
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
