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

package push

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/samber/lo"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/operations"
	"github.com/deckhouse/deckhouse-cli/internal/version"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/validation"
)

// CLI Parameters
var (
	TempDir string

	RegistryHost      string
	RegistryPath      string
	RegistryUsername  string
	RegistryPassword  string
	ModulesPathSuffix string

	Insecure         bool
	TLSSkipVerify    bool
	ImagesBundlePath string
)

var pushLong = `Upload Deckhouse Kubernetes Platform distribution bundle to the third-party registry.

This command pushes the Deckhouse Kubernetes Platform distribution into the specified container registry.

For more information on how to use it, consult the docs at 
https://deckhouse.io/products/kubernetes-platform/documentation/v1/deckhouse-faq.html#manually-uploading-images-to-an-air-gapped-registry

Additional configuration options for the d8 mirror family of commands are available as environment variables:

 * $SSL_CERT_FILE           — Path to the SSL certificate. If the variable is set, system certificates are not used;
 * $SSL_CERT_DIR            — List of directories to search for SSL certificate files, separated by a colon.
                              If set, system certificates are not used. More info at https://docs.openssl.org/1.0.2/man1/c_rehash/;
 * $HTTP_PROXY/$HTTPS_PROXY — URL of the proxy server for HTTP(S) requests to hosts that are not listed in the $NO_PROXY;
 * $NO_PROXY                — Comma-separated list of hosts to exclude from proxying.
                              Supported value formats include IP's', CIDR notations (1.2.3.4/8), domains, and asterisk sign (*).
                              The IP addresses and domain names can also include a literal port number (1.2.3.4:80).
                              The domain name matches that name and all the subdomains.
                              The domain name with a leading . matches subdomains only.
                              For example, foo.com matches foo.com and bar.foo.com; .y.com matches x.y.com but does not match y.com.
                              A single asterisk * indicates that no proxying should be done;

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2025`

func NewCommand() *cobra.Command {
	pushCmd := &cobra.Command{
		Use:           "push <images-bundle-path> <registry>",
		Short:         "Copy Deckhouse Kubernetes Platform distribution to the third-party registry",
		Long:          pushLong,
		ValidArgs:     []string{"images-bundle-path", "registry"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE:          push,
		PostRunE: func(_ *cobra.Command, _ []string) error {
			return os.RemoveAll(TempDir)
		},
	}

	addFlags(pushCmd.Flags())
	return pushCmd
}

func pushModules(pushParams *params.PushParams, logger params.Logger) error {
	bundleContents, err := os.ReadDir(pushParams.BundleDir)
	if err != nil {
		return fmt.Errorf("List bundle directory: %w", err)
	}

	modulePackages := lo.Compact(lo.Map(bundleContents, func(item os.DirEntry, _ int) string {
		fileExt := filepath.Ext(item.Name())
		pkgName, _, ok := strings.Cut(strings.TrimPrefix(item.Name(), "module-"), ".")
		switch {
		case !ok:
			fallthrough
		case fileExt != ".tar" && fileExt != ".chunk":
			fallthrough
		case !strings.HasPrefix(item.Name(), "module-"):
			return ""
		}
		return pkgName
	}))

	successfullyPushedModules := make([]string, 0)
	for _, modulePackageName := range modulePackages {
		if lo.Contains(successfullyPushedModules, modulePackageName) {
			continue
		}

		if err = logger.Process("Push module: "+modulePackageName, func() error {
			pkg, err := openPackage(pushParams, "module-"+modulePackageName)
			if err != nil {
				return fmt.Errorf("Open package %q: %w", modulePackageName, err)
			}
			if err = operations.PushModule(pushParams, modulePackageName, pkg); err != nil {
				return fmt.Errorf("Failed to push module %q: %w", modulePackageName, err)
			}
			successfullyPushedModules = append(successfullyPushedModules, modulePackageName)
			return nil
		}); err != nil {
			logger.WarnLn(err)
		}
	}

	if len(successfullyPushedModules) > 0 {
		logger.Infof("Modules pushed: %v", strings.Join(successfullyPushedModules, ", "))
	}

	return nil
}

func pushStaticPackages(pushParams *params.PushParams, logger params.Logger) error {
	packages := []string{"platform", "security"}
	for _, pkgName := range packages {
		pkg, err := openPackage(pushParams, pkgName)
		switch {
		case errors.Is(err, os.ErrNotExist):
			logger.InfoLn(pkgName, "package is not present, skipping")
			continue
		case err != nil:
			return err
		}

		switch pkgName {
		case "platform":
			if err = logger.Process("Push Deckhouse platform", func() error {
				return operations.PushDeckhousePlatform(pushParams, pkg)
			}); err != nil {
				return fmt.Errorf("Push Deckhouse Platform: %w", err)
			}
		case "security":
			if err = logger.Process("Push security databases", func() error {
				return operations.PushSecurityDatabases(pushParams, pkg)
			}); err != nil {
				return fmt.Errorf("Push Security Databases: %w", err)
			}
		default:
			return errors.New("Unknown package " + pkgName)
		}

		if err = pkg.Close(); err != nil {
			logger.Warnf("Could not close bundle package %s: %w", pkgName, err)
		}
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

func buildPushParams(logger params.Logger) *params.PushParams {
	pushParams := &params.PushParams{
		BaseParams: params.BaseParams{
			Logger:              logger,
			Insecure:            Insecure,
			SkipTLSVerification: TLSSkipVerify,
			RegistryHost:        RegistryHost,
			RegistryPath:        RegistryPath,
			ModulesPathSuffix:   ModulesPathSuffix,
			BundleDir:           ImagesBundlePath,
			WorkingDir:          filepath.Join(TempDir, "push"),
		},

		Parallelism: params.ParallelismConfig{
			Blobs:  4,
			Images: 1,
		},
	}
	return pushParams
}

func validateRegistryAccess(ctx context.Context, pushParams *params.PushParams) error {
	opts := []validation.Option{
		validation.UseAuthProvider(pushParams.RegistryAuth),
		validation.WithInsecure(pushParams.Insecure),
		validation.WithTLSVerificationSkip(pushParams.SkipTLSVerification),
	}

	accessValidator := validation.NewRemoteRegistryAccessValidator()
	err := accessValidator.ValidateWriteAccessForRepo(ctx, path.Join(pushParams.RegistryHost, pushParams.RegistryPath), opts...)
	if err != nil {
		return err
	}

	return nil
}

func openPackage(pushParams *params.PushParams, pkgName string) (io.ReadCloser, error) {
	p := filepath.Join(pushParams.BundleDir, pkgName+".tar")
	pkg, err := os.Open(p)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return openChunkedPackage(pushParams, pkgName)
	case err != nil:
		return nil, fmt.Errorf("Read bundle package %s: %w", pkgName, err)
	}

	return pkg, nil
}

func openChunkedPackage(pushParams *params.PushParams, pkgName string) (io.ReadCloser, error) {
	pkg, err := chunked.Open(pushParams.BundleDir, pkgName+".tar")
	if err != nil {
		return nil, fmt.Errorf("Open bundle package %q: %w", pkgName, err)
	}

	return pkg, nil
}

func push(_ *cobra.Command, _ []string) error {
	logger := setupLogger()
	pushParams := buildPushParams(logger)
	logger.Infof("d8 version: %s", version.Version)
	if RegistryUsername != "" {
		pushParams.RegistryAuth = authn.FromConfig(authn.AuthConfig{Username: RegistryUsername, Password: RegistryPassword})
	}

	logger.InfoLn("Validating registry access")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := validateRegistryAccess(ctx, pushParams); err != nil && os.Getenv("MIRROR_BYPASS_ACCESS_CHECKS") != "1" {
		return fmt.Errorf("registry credentials validation failure: %w", err)
	}

	if err := pushStaticPackages(pushParams, logger); err != nil {
		return err
	}

	if err := pushModules(pushParams, logger); err != nil {
		return err
	}

	return nil
}
