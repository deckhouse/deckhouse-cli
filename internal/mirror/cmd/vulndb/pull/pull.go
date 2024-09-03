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
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

const (
	deckhouseRegistryHost     = "registry.deckhouse.io"
	enterpriseEditionRepoPath = "/deckhouse/ee"

	enterpriseEditionRepo = deckhouseRegistryHost + enterpriseEditionRepoPath
)

var pullLong = templates.LongDesc(`
Pull vulnerability databases to the local filesystem.

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	pullCmd := &cobra.Command{
		Use:           "pull <vulnerability-db-path>",
		Short:         "Pull vulnerability databases to the local filesystem",
		Long:          pullLong,
		ValidArgs:     []string{"vulnerability-db-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE:          pull,
	}

	addFlags(pullCmd.Flags())
	logs.AddFlags(pullCmd.Flags())
	return pullCmd
}

var (
	SourceRegistryRepo     string
	SourceRegistryLogin    string
	SourceRegistryPassword string

	VulnerabilityDBPath string
	LicenseToken        string

	TLSSkipVerify bool
	Insecure      bool
)

func pull(_ *cobra.Command, _ []string) error {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	logger := log.NewSLogger(logLevel)

	pullContext := &contexts.PullContext{
		BaseContext: contexts.BaseContext{
			Logger:                logger,
			RegistryAuth:          getSourceRegistryAuthProvider(),
			DeckhouseRegistryRepo: SourceRegistryRepo,
		},
	}

	var err error
	imageLayouts := &layouts.ImageLayouts{}

	imageLayouts.TrivyDB, err = layouts.CreateEmptyImageLayoutAtPath(filepath.Join(VulnerabilityDBPath, "trivy-db"))
	if err != nil {
		return fmt.Errorf("creating trivy db layout: %w", err)
	}
	imageLayouts.TrivyBDU, err = layouts.CreateEmptyImageLayoutAtPath(filepath.Join(VulnerabilityDBPath, "trivy-bdu"))
	if err != nil {
		return fmt.Errorf("creating bdu layout: %w", err)
	}
	imageLayouts.TrivyJavaDB, err = layouts.CreateEmptyImageLayoutAtPath(filepath.Join(VulnerabilityDBPath, "trivy-java-db"))
	if err != nil {
		return fmt.Errorf("creating java db layout: %w", err)
	}

	if err := layouts.PullTrivyVulnerabilityDatabasesImages(pullContext, imageLayouts); err != nil {
		return fmt.Errorf("pull vulnerability databases: %w", err)
	}

	return nil
}

func getSourceRegistryAuthProvider() authn.Authenticator {
	if SourceRegistryLogin != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: SourceRegistryLogin,
			Password: SourceRegistryPassword,
		})
	}

	if LicenseToken != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: LicenseToken,
		})
	}

	return authn.Anonymous
}
