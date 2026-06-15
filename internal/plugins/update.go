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
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// UpdateAll updates every installed plugin to its newest cluster-compatible
// version within the current major. A per-plugin failure does not stop the
// others; the failures are reported together in the returned error.
func (m *Manager) UpdateAll(ctx context.Context) error {
	plugins, err := m.InstalledPluginNames()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to read plugins directory: %w", err)
	}

	// A non-root install lives in the home fallback (~/.deckhouse-cli), so this
	// update must look there too when the configured root has nothing - otherwise
	// `d8 plugins update all` would be a silent no-op for that install.
	if len(plugins) == 0 && m.switchToFallbackRoot() {
		if plugins, err = m.InstalledPluginNames(); err != nil {
			return fmt.Errorf("failed to read plugins directory: %w", err)
		}
	}

	// Keep going on a per-plugin failure so one plugin (e.g. one gated by an
	// unreachable cluster requirement) does not block updating the rest;
	// report the failures together at the end.
	var failed []string

	for _, plugin := range plugins {
		if err := m.InstallPlugin(ctx, plugin); err != nil {
			fmt.Printf("✗ %s: %v\n", plugin, err)
			failed = append(failed, plugin)
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("failed to update %d plugin(s): %s", len(failed), strings.Join(failed, ", "))
	}

	return nil
}

// switchToFallbackRoot retargets m to the home fallback root (~/.deckhouse-cli)
// when it differs from the configured root and actually holds an install.
// Reports whether the switch happened.
func (m *Manager) switchToFallbackRoot() bool {
	fallback, err := layout.HomeFallbackPath()
	if err != nil {
		return false
	}

	if fallback == m.pluginDirectory || !layout.RootHasInstall(fallback) {
		return false
	}

	m.pluginDirectory = fallback

	return true
}

// InstalledPluginNames returns the plugins that are actually installed under the
// plugins root - a directory with a `current` symlink. A leftover directory from a
// failed install has no symlink and is excluded, so it cannot become an install
// target for a plugin the user never had.
func (m *Manager) InstalledPluginNames() ([]string, error) {
	entries, err := os.ReadDir(layout.PluginsRoot(m.pluginDirectory))
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(entries))

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if _, err := os.Lstat(layout.CurrentLinkPath(m.pluginDirectory, entry.Name())); err != nil {
			continue
		}

		names = append(names, entry.Name())
	}

	return names, nil
}
