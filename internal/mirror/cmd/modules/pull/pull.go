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

package pull

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

var pullLong = templates.LongDesc(`
Download Deckhouse modules images from ModuleSource to local filesystem.

For more information on how to use it, consult the docs at 
https://deckhouse.io/documentation/v1/deckhouse-faq.html#manually-uploading-images-of-deckhouse-modules-into-an-isolated-private-registry

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	mirrorModulesCmd := &cobra.Command{
		Use:           "pull",
		Short:         "Download Deckhouse modules images from ModuleSource to local filesystem",
		Long:          pullLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE:          pull,
	}

	addFlags(mirrorModulesCmd.Flags())
	logs.AddFlags(mirrorModulesCmd.Flags())
	logs.AddFlags(mirrorModulesCmd.PersistentFlags())
	return mirrorModulesCmd
}

var (
	ModulesDirectory string
	ModuleSourcePath string
	ModulesFilter    string

	SkipTLSVerify bool
)

func pull(_ *cobra.Command, _ []string) error {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	logger := log.NewSLogger(logLevel)

	return pullExternalModulesToLocalFS(
		logger,
		ModuleSourcePath,
		ModulesDirectory,
		ModulesFilter,
		SkipTLSVerify,
	)
}

func pullExternalModulesToLocalFS(
	logger contexts.Logger,
	sourceYmlPath, mirrorDirectoryPath, moduleFilterExpression string,
	skipVerifyTLS bool,
) error {
	src, err := loadModuleSourceFromPath(sourceYmlPath)
	if err != nil {
		return fmt.Errorf("Read ModuleSource: %w", err)
	}

	insecure := strings.ToUpper(src.Spec.Registry.Scheme) == "HTTP"
	authProvider, err := findRegistryAuthCredentials(src)
	if err != nil {
		return fmt.Errorf("Parse dockerCfg: %w", err)
	}

	modulesFromRepo, err := modules.GetExternalModulesFromRepo(src.Spec.Registry.Repo, authProvider, insecure, skipVerifyTLS)
	if err != nil {
		return fmt.Errorf("Get external modules from %q: %w", src.Spec.Registry.Repo, err)
	}
	if len(modulesFromRepo) == 0 {
		logger.WarnLn("No modules found in ModuleSource")
		return nil
	}

	modulesFilter, err := modules.NewFilter(moduleFilterExpression, logger)
	if err != nil {
		return fmt.Errorf("Bad modules filter: %w", err)
	}
	if modulesFilter.Len() > 0 {
		filteredModules := make([]modules.Module, 0)
		for _, moduleData := range modulesFromRepo {
			if !modulesFilter.MatchesFilter(&moduleData) {
				continue
			}

			modulesFilter.FilterReleases(&moduleData)
			filteredModules = append(filteredModules, moduleData)
		}
		modulesFromRepo = filteredModules
	}

	tagsResolver := layouts.NewTagsResolver()
	for i, module := range modulesFromRepo {
		logger.InfoF("[%d / %d] Pulling module %s ", i+1, len(modulesFromRepo), module.RegistryPath)

		moduleLayout, err := layouts.CreateEmptyImageLayoutAtPath(filepath.Join(mirrorDirectoryPath, module.Name))
		if err != nil {
			return fmt.Errorf("Create module OCI Layouts: %w", err)
		}
		moduleReleasesLayout, err := layouts.CreateEmptyImageLayoutAtPath(filepath.Join(mirrorDirectoryPath, module.Name, "release"))
		if err != nil {
			return fmt.Errorf("Create module OCI Layouts: %w", err)
		}

		moduleImageSet, releasesImageSet, err := modules.FindExternalModuleImages(&module, modulesFilter, authProvider, insecure, skipVerifyTLS)
		if err != nil {
			return fmt.Errorf("Find external module images`: %w", err)
		}

		for _, imageSet := range []map[string]struct{}{moduleImageSet, releasesImageSet} {
			if err = tagsResolver.ResolveTagsDigestsFromImageSet(imageSet, authProvider, insecure, skipVerifyTLS); err != nil {
				return fmt.Errorf("Resolve digests for images tags: %w", err)
			}
		}

		pullCtx := &contexts.PullContext{
			BaseContext: contexts.BaseContext{
				Logger:              logger,
				Insecure:            insecure,
				SkipTLSVerification: skipVerifyTLS,
				RegistryAuth:        authProvider,
			},
		}

		logger.InfoLn("Pulling module contents")
		err = layouts.PullImageSet(pullCtx, moduleLayout, moduleImageSet, layouts.WithTagToDigestMapper(tagsResolver.GetTagDigest))
		if err != nil {
			return fmt.Errorf("Pull images: %w", err)
		}

		logger.InfoLn("Pulling module release data")
		err = layouts.PullImageSet(pullCtx, moduleReleasesLayout, releasesImageSet, layouts.WithTagToDigestMapper(tagsResolver.GetTagDigest))
		if err != nil {
			return fmt.Errorf("Pull images: %w", err)
		}
	}

	return nil
}

func loadModuleSourceFromPath(sourceYmlPath string) (*v1alpha1.ModuleSource, error) {
	rawYml, err := os.ReadFile(sourceYmlPath)
	if err != nil {
		return nil, fmt.Errorf("Read %q: %w", sourceYmlPath, err)
	}

	src := &v1alpha1.ModuleSource{}
	if err = yaml.Unmarshal(rawYml, src); err != nil {
		return nil, fmt.Errorf("Parse ModuleSource YAML: %w", err)
	}

	if src.Spec.Registry.Scheme == "" {
		src.Spec.Registry.Scheme = "HTTPS"
	}

	return src, nil
}

func findRegistryAuthCredentials(source *v1alpha1.ModuleSource) (authn.Authenticator, error) {
	buf, err := base64.StdEncoding.DecodeString(source.Spec.Registry.DockerCFG)
	if err != nil {
		return nil, fmt.Errorf("Decode dockerCfg: %w", err)
	}

	registryURL, err := url.Parse(strings.ToLower(source.Spec.Registry.Scheme) + "://" + source.Spec.Registry.Repo)
	if err != nil {
		return nil, fmt.Errorf("Malformed ModuleSource: spec.registry: %w", err)
	}

	decodedDockerCfg := struct {
		Auths map[string]struct {
			Auth     string `json:"auth,omitempty"`
			User     string `json:"username,omitempty"`
			Password string `json:"password,omitempty"`
		} `json:"auths"`
	}{}
	if err := json.Unmarshal(buf, &decodedDockerCfg); err != nil {
		return nil, fmt.Errorf("Decode dockerCfg: %w", err)
	}

	if decodedDockerCfg.Auths == nil {
		return authn.Anonymous, nil
	}
	registryAuth, hasRegistryCreds := decodedDockerCfg.Auths[registryURL.Host]
	if !hasRegistryCreds {
		return authn.Anonymous, nil
	}

	if registryAuth.Auth != "" {
		return authn.FromConfig(authn.AuthConfig{
			Auth: registryAuth.Auth,
		}), nil
	}

	if registryAuth.User != "" && registryAuth.Password != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: registryAuth.User,
			Password: registryAuth.Password,
		}), nil
	}

	return authn.Anonymous, nil
}
