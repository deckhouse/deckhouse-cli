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
	"os"
	"path/filepath"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"
)

func parseAndValidateParameters(_ *cobra.Command, args []string) error {
	var err error
	if err = parseAndValidateVersionFlags(); err != nil {
		return err
	}
	if err = validateImagesBundlePathArg(args); err != nil {
		return err
	}
	if err = validateChunkSizeFlag(); err != nil {
		return err
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
	if len(dirEntries) > 0 {
		return fmt.Errorf("%s is not empty, use --force to override", ImagesBundlePath)
	}

	return nil
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
