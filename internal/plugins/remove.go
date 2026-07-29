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
	"os"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// Remove deletes an installed plugin from disk: its install directory and the
// cached contract. It is idempotent (removing a plugin that is not installed is a
// no-op) and holds the plugin's install lock so it cannot race a concurrent
// install of the same plugin.
func (m *Manager) Remove(pluginName string) error {
	if err := ValidatePluginName(pluginName); err != nil {
		return err
	}

	pluginDir := layout.PluginDir(m.pluginDirectory, pluginName)

	// Nothing installed: stay idempotent and skip locking - the lock file lives
	// inside pluginDir, so there is nothing to serialize against and no parent to
	// create it under.
	if _, err := os.Stat(pluginDir); errors.Is(err, os.ErrNotExist) {
		fmt.Printf("Plugin '%s' is not installed.\n", pluginName)

		return nil
	}

	return m.removeLocked(pluginName, pluginDir)
}

// RemoveAll deletes every plugin found under the plugins root, each under its own
// install lock.
func (m *Manager) RemoveAll() error {
	plugins, err := os.ReadDir(layout.PluginsRoot(m.pluginDirectory))
	if err != nil {
		return fmt.Errorf("failed to read plugins directory: %w", err)
	}

	fmt.Println("Found", len(plugins), "plugins to remove:")

	for _, plugin := range plugins {
		if !plugin.IsDir() {
			continue
		}

		if err := m.removeLocked(plugin.Name(), layout.PluginDir(m.pluginDirectory, plugin.Name())); err != nil {
			return err
		}
	}

	return nil
}

// removeLocked deletes one plugin's install directory and cached contract while
// holding the plugin's install lock. The same lock guards installs, so a remove
// can no longer delete a directory out from under a concurrent install (which
// would corrupt it); a concurrent install instead fails fast with the lock error.
func (m *Manager) removeLocked(pluginName, pluginDir string) error {
	release, err := m.acquireInstallLock(layout.InstallLockPath(m.pluginDirectory, pluginName))
	if err != nil {
		return err
	}

	defer release()

	fmt.Printf("Removing plugin from: %s\n", pluginDir)

	if err := os.RemoveAll(pluginDir); err != nil {
		return fmt.Errorf("failed to remove plugin directory: %w", err)
	}

	fmt.Println("Cleaning up plugin files...")

	_ = os.Remove(layout.ContractFile(m.pluginDirectory, pluginName))

	fmt.Printf("✓ Plugin '%s' successfully removed!\n", pluginName)

	return nil
}
