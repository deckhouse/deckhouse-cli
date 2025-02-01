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
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
	"github.com/samber/lo/parallel"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/gostsums"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/operations"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/releases"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/validation"
)

var ErrPullFailed = errors.New("pull failed, see the log for details")

// CLI Parameters
var (
	TempDir = filepath.Join(os.TempDir(), "mirror")

	Insecure      bool
	TLSSkipVerify bool
	ForcePull     bool

	ImagesBundlePath        string
	ImagesBundleChunkSizeGB int64

	sinceVersionString string
	SinceVersion       *semver.Version

	DeckhouseTag string

	ModulesPathSuffix string
	ModulesWhitelist  []string
	ModulesBlacklist  []string

	SourceRegistryRepo     = enterpriseEditionRepo // Fallback to EE if nothing was given as source.
	SourceRegistryLogin    string
	SourceRegistryPassword string
	DeckhouseLicenseToken  string

	DoGOSTDigest bool
	NoPullResume bool

	NoPlatform   bool
	NoSecurityDB bool
	NoModules    bool
)

var pullLong = templates.LongDesc(`
Download Deckhouse Kubernetes Platform distribution to the local filesystem.
		
This command downloads the Deckhouse Kubernetes Platform distribution bundle 
containing specific platform releases and it's modules, 
to be pushed into the air-gapped container registry at a later time.

For more information on how to use it, consult the docs at 
https://deckhouse.io/products/kubernetes-platform/documentation/v1/deckhouse-faq.html#manually-uploading-images-to-an-air-gapped-registry

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	pullCmd := &cobra.Command{
		Use:           "pull <images-bundle-path>",
		Short:         "Copy Deckhouse Kubernetes Platform distribution to the local filesystem",
		Long:          pullLong,
		ValidArgs:     []string{"images-bundle-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE:          pull,
		PostRunE: func(_ *cobra.Command, _ []string) error {
			return os.RemoveAll(TempDir)
		},
	}

	addFlags(pullCmd.Flags())
	return pullCmd
}

func pull(cmd *cobra.Command, _ []string) error {
	logger := setupLogger()
	pullParams := buildPullParams(logger)

	if NoPullResume || lastPullWasTooLongAgoToRetry(pullParams) {
		if err := os.RemoveAll(pullParams.WorkingDir); err != nil {
			return fmt.Errorf("Cleanup last unfinished pull data: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()
	if err := validateRegistryAccess(ctx, pullParams); err != nil && os.Getenv("MIRROR_BYPASS_ACCESS_CHECKS") != "1" {
		return fmt.Errorf("Source registry access validation failure: %w", err)
	}

	if !pullParams.SkipPlatform {
		if err := logger.Process("Pull Deckhouse Kubernetes Platform", func() error {
			tagsToMirror, err := findTagsToMirror(pullParams, logger)
			if err != nil {
				return fmt.Errorf("Find tags to mirror: %w", err)
			}
			if err = operations.PullDeckhousePlatform(pullParams, tagsToMirror); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return ErrPullFailed
		}
	}

	if !pullParams.SkipSecurityDatabases {
		if err := logger.Process("Pull Security Databases", func() error {
			if err := operations.PullSecurityDatabases(pullParams); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return ErrPullFailed
		}
	}

	if !pullParams.SkipModules {
		if err := logger.Process("Pull Modules", func() error {
			filterExpressions := ModulesBlacklist
			filterType := modules.FilterTypeBlacklist
			if ModulesWhitelist != nil {
				filterExpressions = ModulesWhitelist
				filterType = modules.FilterTypeWhitelist
			}

			filter, err := modules.NewFilter(filterExpressions, filterType)
			if err != nil {
				return fmt.Errorf("Prepare module filter: %w", err)
			}
			return operations.PullModules(pullParams, filter)
		}); err != nil {
			return ErrPullFailed
		}
	}

	if !DoGOSTDigest {
		return nil
	}

	if err := logger.Process("Compute GOST digests for bundle", func() error {
		bundleDirContents, err := os.ReadDir(pullParams.BundleDir)
		if err != nil {
			return fmt.Errorf("Read Deckhouse Kubernetes Platform distribution bundle: %w", err)
		}

		bundlePackages := lo.Filter(bundleDirContents, func(item os.DirEntry, _ int) bool {
			ext := filepath.Ext(item.Name())
			return ext == ".tar" || ext == ".chunk"
		})

		merr := &multierror.Error{}
		parallel.ForEach(bundlePackages, func(bundlePackage os.DirEntry, _ int) {
			file, err := os.Open(filepath.Join(pullParams.BundleDir, bundlePackage.Name()))
			if err != nil {
				merr = multierror.Append(merr, fmt.Errorf("Read Deckhouse Kubernetes Platform distribution bundle: %w", err))
			}

			digest, err := gostsums.CalculateBlobGostDigest(file)
			if err != nil {
				merr = multierror.Append(merr, fmt.Errorf("Calculate digest: %w", err))
			}

			if err = os.WriteFile(
				filepath.Join(pullParams.BundleDir, bundlePackage.Name())+".gostsum",
				[]byte(digest),
				0644,
			); err != nil {
				merr = multierror.Append(merr, fmt.Errorf("Could not write digest to .gostsum file: %w", err))
			}
		})
		return merr.ErrorOrNil()
	}); err != nil {
		return fmt.Errorf("Compute GOST digests for bundle: %w", err)
	}

	return nil
}

func setupLogger() *log.SLogger {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	return log.NewSLogger(logLevel)
}

func findTagsToMirror(pullParams *params.PullParams, logger *log.SLogger) ([]string, error) {
	if pullParams.DeckhouseTag != "" {
		logger.InfoF("Skipped releases lookup as tag %q is specifically requested with --deckhouse-tag", pullParams.DeckhouseTag)
		return []string{pullParams.DeckhouseTag}, nil
	}

	versionsToMirror, err := releases.VersionsToMirror(pullParams)
	if err != nil {
		return nil, fmt.Errorf("Find versions to mirror: %w", err)
	}
	logger.InfoF("Deckhouse releases to pull: %+v", versionsToMirror)

	return lo.Map(versionsToMirror, func(v semver.Version, index int) string {
		return "v" + v.String()
	}), nil
}

func buildPullParams(logger params.Logger) *params.PullParams {
	mirrorCtx := &params.PullParams{
		BaseParams: params.BaseParams{
			Logger:                logger,
			Insecure:              Insecure,
			SkipTLSVerification:   TLSSkipVerify,
			DeckhouseRegistryRepo: SourceRegistryRepo,
			RegistryAuth:          getSourceRegistryAuthProvider(),
			BundleDir:             ImagesBundlePath,
			WorkingDir: filepath.Join(
				TempDir,
				"pull",
				fmt.Sprintf("%x", md5.Sum([]byte(SourceRegistryRepo))),
			),
		},

		BundleChunkSize: ImagesBundleChunkSizeGB * 1000 * 1000 * 1000,

		ModulesPathSuffix: ModulesPathSuffix,

		DoGOSTDigests:         DoGOSTDigest,
		SkipPlatform:          NoPlatform,
		SkipSecurityDatabases: NoSecurityDB,
		SkipModules:           NoModules,
		DeckhouseTag:          DeckhouseTag,
		SinceVersion:          SinceVersion,
	}
	return mirrorCtx
}

func getSourceRegistryAuthProvider() authn.Authenticator {
	if SourceRegistryLogin != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: SourceRegistryLogin,
			Password: SourceRegistryPassword,
		})
	}

	if DeckhouseLicenseToken != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: DeckhouseLicenseToken,
		})
	}

	return authn.Anonymous
}

func lastPullWasTooLongAgoToRetry(pullParams *params.PullParams) bool {
	s, err := os.Lstat(pullParams.WorkingDir)
	if err != nil {
		return false
	}

	return time.Since(s.ModTime()) > 24*time.Hour
}

func validateRegistryAccess(ctx context.Context, pullParams *params.PullParams) error {
	targetTag := "alpha"
	if pullParams.DeckhouseTag != "" {
		targetTag = pullParams.DeckhouseTag
	}

	opts := []validation.Option{
		validation.UseAuthProvider(pullParams.RegistryAuth),
	}
	if pullParams.Insecure {
		opts = append(opts, validation.UsePlainHTTP())
	}
	if pullParams.SkipTLSVerification {
		opts = append(opts, validation.SkipTLSVerification())
	}

	accessValidator := validation.NewRemoteRegistryAccessValidator()
	err := accessValidator.ValidateReadAccessForImage(ctx, pullParams.DeckhouseRegistryRepo+":"+targetTag, opts...)
	if err != nil {
		return err
	}

	return nil
}
