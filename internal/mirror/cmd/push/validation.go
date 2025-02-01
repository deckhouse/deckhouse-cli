package push

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

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
		return nil
	}

	if bundleExtension := filepath.Ext(ImagesBundlePath); bundleExtension == ".tar" || bundleExtension == ".chunk" {
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
