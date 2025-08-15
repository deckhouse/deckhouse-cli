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

package operations

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

func PullModules(pullParams *params.PullParams, filter *modules.Filter) error {
	var err error
	var modulesData []modules.Module
	logger := pullParams.Logger
	imageLayouts := layouts.NewImageLayouts()
	tmpDir := filepath.Join(pullParams.WorkingDir, "modules")

	logger.InfoLn("Fetching Deckhouse modules list")
	modulesData, err = modules.ForRepo(
		path.Join(pullParams.DeckhouseRegistryRepo, pullParams.ModulesPathSuffix),
		pullParams.RegistryAuth,
		pullParams.Insecure, pullParams.SkipTLSVerification)
	if err != nil {
		return fmt.Errorf("Find modules: %w", err)
	}

	if len(modulesData) == 0 {
		logger.WarnLn("Modules were not found, check your source repository address and modules path suffix")
		return nil
	}
	printModulesList(logger, modulesData)

	logger.InfoLn("Creating OCI Layouts")
	for _, module := range modulesData {
		if !filter.Match(&module) {
			continue
		}

		moduleLayout, err := layouts.CreateEmptyImageLayout(filepath.Join(tmpDir, module.Name))
		if err != nil {
			return fmt.Errorf("create OCI layout: %w", err)
		}
		releasesLayout, err := layouts.CreateEmptyImageLayout(filepath.Join(tmpDir, module.Name, "release"))
		if err != nil {
			return fmt.Errorf("create OCI layout: %w", err)
		}
		imageLayouts.Modules[module.Name] = layouts.ModuleImageLayout{
			ModuleLayout:   moduleLayout,
			ReleasesLayout: releasesLayout,
			ModuleImages:   make(map[string]struct{}),
			ReleaseImages:  make(map[string]struct{}),
		}
	}

	logger.InfoLn("Searching for Deckhouse external modules images")
	if err = layouts.FindDeckhouseModulesImages(pullParams, imageLayouts, modulesData, filter); err != nil {
		return fmt.Errorf("Find modules images: %w", err)
	}

	if err = logger.Process("Pull images", func() error {
		return layouts.PullModules(pullParams, imageLayouts)
	}); err != nil {
		return err
	}

	logger.InfoLn("Processing image indexes")
	for _, l := range imageLayouts.AsList() {
		err = layouts.SortIndexManifests(l)
		if err != nil {
			return fmt.Errorf("Sorting index manifests of %s: %w", l, err)
		}
	}

	for name, layout := range imageLayouts.Modules {
		
		if err := ApplyChannelAliasesIfNeeded(name, layout, filter); err != nil {
			return  err
		}

		pkgName := "module-" + name + ".tar"
		logger.InfoF("Packing %s", pkgName)

		var pkg io.Writer = chunked.NewChunkedFileWriter(pullParams.BundleChunkSize, pullParams.BundleDir, pkgName)
		if pullParams.BundleChunkSize == 0 {
			pkg, err = os.Create(filepath.Join(pullParams.BundleDir, pkgName))
			if err != nil {
				return fmt.Errorf("Create %s: %w", pkgName, err)
			}
		}

		if err = bundle.Pack(context.Background(), string(layout.ModuleLayout), pkg); err != nil {
			return fmt.Errorf("Pack module %s: %w", pkgName, err)
		}
	}

	return nil
}

func ApplyChannelAliasesIfNeeded(name string, layout layouts.ModuleImageLayout, filter *modules.Filter) (error) {
	if c, ok := filter.GetConstraint(name); ok && c.HasChannelAlias() {
		ex, _ := c.(*modules.ExactTagConstraint)
		desc, err := layouts.FindImageDescriptorByTag(layout.ReleasesLayout, ex.Tag())
		if err != nil {
			return err
		}
		if err := layouts.TagImage(layout.ReleasesLayout, desc.Digest, ex.Channel()); err != nil {
			return err
		}
	}
	return nil
}

func printModulesList(logger params.Logger, modulesData []modules.Module) {
	logger.InfoF("Repo contains %d modules:", len(modulesData))
	for i, module := range modulesData {
		logger.InfoF("%d:\t%s", i+1, module.Name)
	}
}
