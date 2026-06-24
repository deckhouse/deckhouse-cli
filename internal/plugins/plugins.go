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

package plugins

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
)

// Manager is the plugin machinery shared by every `d8 plugins ...` subcommand
// and the per-plugin wrapper command (see internal/plugins/cmd): it installs,
// updates, removes, lists and runs plugins from the configured source.
type Manager struct {
	service         pluginSource
	pluginDirectory string

	// clusterStateCache memoizes the cluster snapshot used to enforce cluster-side
	// requirements, so a single command run inspects the cluster at most once.
	clusterStateCache *requirements.ClusterState

	// contractCache memoizes plugin contracts by name@tag so requirements-aware
	// version selection does not re-pull the same image within a command run.
	// Not safe for concurrent use - Manager is per-invocation, run sequentially.
	contractCache map[string]*internal.Plugin

	// tagsCache memoizes a plugin's published tags by name. The dependency planner
	// probes the same dep across several candidate paths, so this keeps the tag
	// listing to one registry call per plugin within a command run.
	tagsCache map[string][]string

	// builtins are d8 built-in command names that satisfy a plugin dependency of
	// the same name by their mere presence - a bridge for capabilities not yet
	// shipped as standalone plugins (e.g. delivery-kit). Set via
	// SetBuiltinCommands; nil for Managers that never resolve dependencies.
	builtins map[string]struct{}

	logger *dkplog.Logger
}

func NewManager(logger *dkplog.Logger) *Manager {
	return &Manager{
		pluginDirectory: flags.DeckhousePluginsDir,
		logger:          logger,
	}
}

// SetDirectory retargets the manager at another install root. The command layer
// calls it after flag parsing, so --plugins-dir overrides the directory captured
// at construction time.
func (m *Manager) SetDirectory(dir string) {
	m.pluginDirectory = dir
}

// EnsureInstallRoot creates <pluginDirectory>/plugins; on permission denied
// falls back to ~/.deckhouse-cli, updates m.pluginDirectory, and retries.
func (m *Manager) EnsureInstallRoot() error {
	err := os.MkdirAll(layout.PluginsRoot(m.pluginDirectory), 0755)
	if err == nil {
		return nil
	}

	if !errors.Is(err, os.ErrPermission) {
		return err
	}

	m.logger.Debug("use homedir instead of default d8 plugins path in '/opt/deckhouse/lib/deckhouse-cli'",
		slog.String("was", m.pluginDirectory), dkplog.Err(err))

	fallback, ferr := layout.HomeFallbackPath()
	if ferr != nil {
		return fmt.Errorf("home fallback: %w", ferr)
	}

	m.pluginDirectory = fallback

	return os.MkdirAll(layout.PluginsRoot(m.pluginDirectory), 0755)
}
