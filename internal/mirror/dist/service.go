/*
Copyright 2026 Flant JSC

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

package dist

import (
	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// Options contains configuration options for the dist service.
type Options struct {
	// CLITag pins the deckhouse-cli version to mirror, instead of the newest
	// published stable one. Empty means automatic selection.
	CLITag string
	// Filter carries --include-plugin expressions (whitelist, additive to the
	// module-driven auto-selection). May be nil.
	Filter *modules.Filter
	// Builtins are d8 built-in command names that satisfy a same-named plugin
	// dependency by presence (never pulled).
	Builtins map[string]struct{}
	// BundleDir is the directory to store the bundle.
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking).
	BundleChunkSize int64
	// DryRun prints the pull plan without downloading any image blobs.
	DryRun bool
	// ProxyRegistry means the registry serves no catalog: auto-selection is
	// impossible, only explicit exact pins are resolved.
	ProxyRegistry bool
}

// Service is the deckhouse-cli phase of mirror pull: mirror the d8 binary
// itself, resolve which plugin versions the bundle needs and pull them
// (multi-platform indexes whole), packing deckhouse-cli.tar and one
// plugin-<name>.tar per plugin.
type Service struct {
	workingDir string

	// cliService handles deckhouse-cli binary registry operations.
	cliService *registryservice.CLIService
	// pluginsService handles plugin registry operations.
	pluginsService *registryservice.PluginsService
	// resolver picks the plugin versions to mirror.
	resolver Resolver
	// layouts holds per-plugin OCI layouts, created lazily.
	layouts map[pluginName]*regimage.ImageLayout
	// cliLayout holds the deckhouse-cli OCI layout, created lazily.
	cliLayout *regimage.ImageLayout

	options *Options

	// pluginStats accumulates plugin pull accounting for the summary.
	pluginStats *pluginsPullStats
	// cliStats accumulates deckhouse-cli pull accounting for the summary.
	cliStats *cliPullStats

	logger     *dkplog.Logger
	userLogger *log.SLogger
}

// NewService creates the deckhouse-cli phase service.
func NewService(
	registryService *registryservice.Service,
	workingDir string,
	options *Options,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	pluginsService := registryService.PluginService()

	return &Service{
		workingDir: workingDir,

		cliService:     registryService.CLIService(),
		pluginsService: pluginsService,
		resolver:       NewResolver(NewCatalog(pluginsService, logger), logger),
		layouts:        make(map[pluginName]*regimage.ImageLayout),

		options: options,

		pluginStats: newPluginsPullStats(),
		cliStats:    &cliPullStats{},

		logger:     logger,
		userLogger: userLogger,
	}
}
