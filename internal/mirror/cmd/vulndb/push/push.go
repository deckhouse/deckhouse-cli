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
	"path"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

var pushLong = templates.LongDesc(`
Push vulnerability databases to the third-party registry.

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	pushCmd := &cobra.Command{
		Use:           "push <vulnerability-db-path> <registry-url>",
		Short:         "Push vulnerability databases to the third-party registry",
		Long:          pushLong,
		ValidArgs:     []string{"vulnerability-db-path", "registry-url"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE:          push,
	}

	addFlags(pushCmd.Flags())
	logs.AddFlags(pushCmd.Flags())
	return pushCmd
}

var (
	RegistryRepo     string
	RegistryHost     string
	RegistryPath     string
	RegistryLogin    string
	RegistryPassword string

	VulnerabilityDBPath string

	TLSSkipVerify bool
	Insecure      bool
)

func push(_ *cobra.Command, _ []string) error {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	logger := log.NewSLogger(logLevel)

	pushContext := &contexts.PushContext{
		BaseContext: contexts.BaseContext{
			Logger:                logger,
			RegistryAuth:          getRegistryAuthProvider(),
			RegistryHost:          RegistryHost,
			RegistryPath:          RegistryPath,
			DeckhouseRegistryRepo: RegistryRepo,
		},
	}

	layoutsPathsAndRepos := map[string]string{
		path.Join(RegistryRepo, "security", "trivy-db"):      filepath.Join(VulnerabilityDBPath, "trivy-db"),
		path.Join(RegistryRepo, "security", "trivy-bdu"):     filepath.Join(VulnerabilityDBPath, "trivy-bdu"),
		path.Join(RegistryRepo, "security", "trivy-java-db"): filepath.Join(VulnerabilityDBPath, "trivy-java-db"),
	}

	repoCount := 0
	for repo, layoutPath := range layoutsPathsAndRepos {
		repoCount++
		logger.InfoF("Pushing repo %d of %d at %s", repoCount, len(layoutsPathsAndRepos), repo)

		ociLayout, err := layout.FromPath(layoutPath)
		if err != nil {
			return fmt.Errorf("load OCI layout at %q: %w", layoutPath, err)
		}

		err = layouts.PushLayoutToRepo(
			ociLayout,
			repo,
			pushContext.RegistryAuth,
			pushContext.Logger,
			pushContext.Insecure,
			pushContext.SkipTLSVerification,
		)
		if err != nil {
			return fmt.Errorf("failed to push vulnerability databases: %w", err)
		}

		logger.InfoLn("Repo", repo, "pushed successfully")
		logger.InfoLn()
	}

	return nil
}

func getRegistryAuthProvider() authn.Authenticator {
	if RegistryLogin != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: RegistryLogin,
			Password: RegistryPassword,
		})
	}

	return authn.Anonymous
}
