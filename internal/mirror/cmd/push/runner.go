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
	"time"

	"github.com/google/go-containerregistry/pkg/authn"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/adapters"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/platform"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/security"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/validation"
)

// Runner is the composition root for the push command
type Runner struct {
	config *Config
	opts   *usecase.PushOpts
	logger usecase.Logger
}

// NewRunner creates a new Runner from CLI flags
func NewRunner() *Runner {
	config := NewConfigFromFlags()
	logger := createPushLogger()

	return &Runner{
		config: config,
		opts:   config.ToPushOpts(),
		logger: adapters.NewLoggerAdapter(logger),
	}
}

// Run executes the push operation
func (r *Runner) Run(ctx context.Context) error {
	r.logger.Info("Starting push operation")

	// Validate registry access
	if err := r.validateRegistryAccess(ctx); err != nil {
		if os.Getenv("MIRROR_BYPASS_ACCESS_CHECKS") != "1" {
			return fmt.Errorf("registry access validation failed: %w", err)
		}
		r.logger.Warnf("Registry access validation failed (bypassed): %v", err)
	}

	// Create registry client
	client, err := r.createRegistryClient()
	if err != nil {
		return fmt.Errorf("create registry client: %w", err)
	}

	// Create push params for legacy operations
	pushParams := r.buildPushParams()

	// Create bundle opener
	bundleOpener := &bundleOpenerImpl{bundleDir: r.config.BundleDir}

	// Create domain services
	platformPusher := platform.NewPlatformPushService(
		bundleOpener,
		platform.NewLegacyPlatformPusher(pushParams, client),
		r.logger,
		&platform.PushOptions{
			BundleDir:  r.config.BundleDir,
			WorkingDir: r.config.WorkingDir,
		},
	)

	securityPusher := security.NewSecurityPushService(
		bundleOpener,
		security.NewLegacySecurityPusher(pushParams, client),
		r.logger,
	)

	// Create modules client with path suffix
	modulesClient := client
	if r.config.ModulesPathSuffix != "" {
		modulesClient = client.WithSegment(r.config.ModulesPathSuffix)
	}

	modulesPusher := modules.NewModulesPushService(
		bundleOpener,
		modules.NewLegacyModulePusher(pushParams, modulesClient),
		r.logger,
		&modules.ModulesPushOptions{BundleDir: r.config.BundleDir},
	)

	// Create use case
	pushUseCase := usecase.NewPushUseCase(
		platformPusher,
		modulesPusher,
		securityPusher,
		r.logger,
		r.opts,
	)

	// Execute
	if err := pushUseCase.Execute(ctx); err != nil {
		return fmt.Errorf("push failed: %w", err)
	}

	r.logger.Info("Push completed successfully")
	return nil
}

func (r *Runner) validateRegistryAccess(ctx context.Context) error {
	r.logger.Info("Validating registry access")

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	opts := []validation.Option{
		validation.WithInsecure(r.config.Registry.Insecure),
		validation.WithTLSVerificationSkip(r.config.Registry.SkipTLSVerify),
	}

	if r.config.Registry.Auth != nil {
		opts = append(opts, validation.UseAuthProvider(r.config.Registry.Auth))
	}

	validator := validation.NewRemoteRegistryAccessValidator()
	repoPath := path.Join(r.config.Registry.Host, r.config.Registry.Path)

	return validator.ValidateWriteAccessForRepo(ctx, repoPath, opts...)
}

func (r *Runner) createRegistryClient() (registry.Client, error) {
	logger := dkplog.NewNop()
	if log.DebugLogLevel() >= 3 {
		logger = dkplog.NewLogger(dkplog.WithLevel(slog.LevelDebug))
	}

	clientOpts := &regclient.Options{
		Insecure:      r.config.Registry.Insecure,
		TLSSkipVerify: r.config.Registry.SkipTLSVerify,
		Logger:        logger,
	}

	if r.config.Registry.Auth != nil {
		clientOpts.Auth = r.config.Registry.Auth
	}

	var client registry.Client = regclient.NewClientWithOptions(r.config.Registry.Host, clientOpts)

	if r.config.Registry.Path != "" {
		client = client.WithSegment(r.config.Registry.Path)
	}

	return client, nil
}

func (r *Runner) buildPushParams() *params.PushParams {
	var auth authn.Authenticator
	if r.config.Registry.Auth != nil {
		auth = r.config.Registry.Auth
	}

	return &params.PushParams{
		BaseParams: params.BaseParams{
			Insecure:            r.config.Registry.Insecure,
			SkipTLSVerification: r.config.Registry.SkipTLSVerify,
			RegistryHost:        r.config.Registry.Host,
			RegistryPath:        r.config.Registry.Path,
			ModulesPathSuffix:   r.config.ModulesPathSuffix,
			BundleDir:           r.config.BundleDir,
			WorkingDir:          r.config.WorkingDir,
			RegistryAuth:        auth,
			Logger:              createPushLogger(),
		},
		Parallelism: params.ParallelismConfig{
			Blobs:  r.config.BlobParallelism,
			Images: r.config.ImageParallelism,
		},
	}
}

func createPushLogger() *log.SLogger {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	return log.NewSLogger(logLevel)
}

// bundleOpenerImpl implements BundleOpener
type bundleOpenerImpl struct {
	bundleDir string
}

func (o *bundleOpenerImpl) Open(pkgName string) (io.ReadCloser, error) {
	p := filepath.Join(o.bundleDir, pkgName+".tar")
	pkg, err := os.Open(p)
	if err == nil {
		return pkg, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		// Try chunked package
		return chunked.Open(o.bundleDir, pkgName+".tar")
	}

	return nil, fmt.Errorf("open package %s: %w", pkgName, err)
}
