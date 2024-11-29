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
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

var pushLong = templates.LongDesc(`
Upload Deckhouse modules images from ModuleSource to local filesystem and to third-party registry.
		
This command is used to manually upload Deckhouse modules images to an air-gapped registry.

For more information on how to use it, consult the docs at 
https://deckhouse.io/documentation/v1/deckhouse-faq.html#manually-uploading-images-of-deckhouse-modules-into-an-isolated-private-registry

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	mirrorModulesCmd := &cobra.Command{
		Use:           "push",
		Short:         "Upload Deckhouse modules images from local filesystem to third-party registry",
		Long:          pushLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE:          push,
	}

	addFlags(mirrorModulesCmd.PersistentFlags())
	return mirrorModulesCmd
}

var (
	MirrorModulesDirectory string

	MirrorModulesRegistry         string
	MirrorModulesRegistryUsername string
	MirrorModulesRegistryPassword string

	MirrorModulesInsecure      bool
	MirrorModulesTLSSkipVerify bool
)

func push(_ *cobra.Command, _ []string) error {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	logger := log.NewSLogger(logLevel)

	var authProvider authn.Authenticator = nil
	if MirrorModulesRegistryUsername != "" {
		authProvider = authn.FromConfig(authn.AuthConfig{
			Username: MirrorModulesRegistryUsername,
			Password: MirrorModulesRegistryPassword,
		})
	}

	return pushModulesToRegistry(
		logger,
		MirrorModulesDirectory,
		MirrorModulesRegistry,
		authProvider,
		MirrorModulesInsecure,
		MirrorModulesTLSSkipVerify,
	)
}

func pushModulesToRegistry(
	logger contexts.Logger,
	modulesDir string,
	registryPath string,
	authProvider authn.Authenticator,
	insecure, skipVerifyTLS bool,
) error {
	dirEntries, err := os.ReadDir(modulesDir)
	if err != nil {
		return fmt.Errorf("Read modules directory: %w", err)
	}

	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authProvider, insecure, skipVerifyTLS)

	for i, entry := range dirEntries {
		if !entry.IsDir() {
			continue
		}

		moduleName := entry.Name()
		moduleRegistryPath := path.Join(registryPath, moduleName)
		moduleReleasesRegistryPath := path.Join(registryPath, moduleName, "release")

		logger.InfoF("Pushing module %s [%d / %d]", moduleName, i+1, len(dirEntries))

		moduleLayout, err := layout.FromPath(filepath.Join(modulesDir, moduleName))
		if err != nil {
			return fmt.Errorf("Module %s: Read OCI layout: %w", moduleName, err)
		}
		moduleReleasesLayout, err := layout.FromPath(filepath.Join(modulesDir, moduleName, "release"))
		if err != nil {
			return fmt.Errorf("Module %s: Read OCI layout: %w", moduleName, err)
		}

		if err = layouts.PushLayoutToRepo(
			moduleLayout,
			moduleRegistryPath,
			authProvider,
			logger,
			contexts.DefaultParallelism,
			insecure,
			skipVerifyTLS,
		); err != nil {
			return fmt.Errorf("Push module to registry: %w", err)
		}

		logger.InfoF("Pushing releases for module %s", moduleName)
		if err = layouts.PushLayoutToRepo(
			moduleReleasesLayout,
			moduleReleasesRegistryPath,
			authProvider,
			logger,
			contexts.DefaultParallelism,
			insecure,
			skipVerifyTLS,
		); err != nil {
			return fmt.Errorf("Push module to registry: %w", err)
		}

		logger.InfoF("Pushing index tag for module %s", moduleName)

		imageRef, err := name.ParseReference(registryPath+":"+moduleName, refOpts...)
		if err != nil {
			return fmt.Errorf("Parse image reference: %w", err)
		}

		img, err := random.Image(16, 1)
		if err != nil {
			return fmt.Errorf("random.Image: %w", err)
		}

		if err = remote.Write(imageRef, img, remoteOpts...); err != nil {
			return fmt.Errorf("Write module index tag: %w", err)
		}

		logger.InfoF("Module %s pushed successfully", moduleName)
	}

	return nil
}
