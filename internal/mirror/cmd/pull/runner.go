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
	"fmt"
	"log/slog"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/adapters"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/platform"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/security"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// Runner is the composition root for the pull command
// It creates and wires all dependencies using Clean Architecture
type Runner struct {
	config *Config
	opts   *usecase.PullOpts
	logger usecase.Logger
}

// NewRunner creates a new Runner from CLI flags
func NewRunner() (*Runner, error) {
	// Build configuration from flags
	config, err := NewConfigFromFlags()
	if err != nil {
		return nil, fmt.Errorf("build config: %w", err)
	}

	// Create logger
	logger := createLogger()

	return &Runner{
		config: config,
		opts:   config.ToPullOpts(),
		logger: adapters.NewLoggerAdapter(logger),
	}, nil
}

// Run executes the pull operation
func (r *Runner) Run(ctx context.Context) error {
	r.logger.Info("Starting pull operation")

	// Create registry client
	registryService, err := r.createRegistryService()
	if err != nil {
		return fmt.Errorf("create registry service: %w", err)
	}

	// Create registry service adapter
	registryAdapter := adapters.NewRegistryServiceAdapter(registryService)

	// Create bundle packer
	bundlePacker := adapters.NewBundlePackerAdapter(
		r.opts.BundleDir,
		r.opts.BundleChunkSize,
		r.logger,
	)

	// Create domain services
	platformService := platform.NewPlatformService(
		registryAdapter,
		bundlePacker,
		r.logger,
		r.opts.NewPlatformOpts(),
	)

	securityService := security.NewSecurityService(
		registryAdapter,
		bundlePacker,
		r.logger,
		r.opts.NewSecurityOpts(),
	)

	modulesService := modules.NewModulesService(
		registryAdapter,
		bundlePacker,
		r.logger,
		r.opts.NewModulesOpts(),
	)

	// Create use case
	pullUseCase := usecase.NewPullUseCase(
		platformService,
		modulesService,
		securityService,
		bundlePacker,
		r.logger,
		r.opts,
	)

	// Execute
	if err := pullUseCase.Execute(ctx); err != nil {
		return fmt.Errorf("pull failed: %w", err)
	}

	r.logger.Info("Pull completed successfully")
	return nil
}

func (r *Runner) createRegistryService() (*registryservice.Service, error) {
	// Create logger for registry client
	logger := dkplog.NewNop()
	if log.DebugLogLevel() >= 3 {
		logger = dkplog.NewLogger(dkplog.WithLevel(slog.LevelDebug))
	}

	// Build client options
	clientOpts := &regclient.Options{
		Insecure:      r.config.Registry.Insecure,
		TLSSkipVerify: r.config.Registry.SkipTLSVerify,
		Logger:        logger,
	}

	// Set auth if provided
	if r.config.Registry.Auth != nil {
		clientOpts.Auth = r.config.Registry.Auth
	}

	// Create client
	client := regclient.NewClientWithOptions(r.config.Registry.URL, clientOpts)

	// Create service
	return registryservice.NewService(client, logger), nil
}

func createLogger() *log.SLogger {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	return log.NewSLogger(logLevel)
}
