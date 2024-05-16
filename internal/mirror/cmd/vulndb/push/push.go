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
	"fmt"
	"path"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/contexts"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/layouts"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/log"
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
	pushContext := &contexts.PushContext{
		BaseContext: contexts.BaseContext{
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
		log.InfoF("Pushing repo %d of %d at %s...\n", repoCount, len(layoutsPathsAndRepos), repo)

		ociLayout, err := layout.FromPath(layoutPath)
		if err != nil {
			return fmt.Errorf("load OCI layout at %q: %w", layoutPath, err)
		}

		err = layouts.PushLayoutToRepo(
			ociLayout,
			repo,
			pushContext.RegistryAuth,
			pushContext.Insecure,
			pushContext.SkipTLSVerification,
		)
		if err != nil {
			return fmt.Errorf("failed to push vulnerability databases: %w", err)
		}

		log.InfoLn("Repo", repo, "pushed successfully")
		log.InfoLn()
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
