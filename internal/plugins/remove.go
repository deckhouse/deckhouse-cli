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
	"fmt"
	"os"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// Remove deletes an installed plugin from disk: its install directory and the
// cached contract.
func (m *Manager) Remove(pluginName string) error {
	if err := ValidatePluginName(pluginName); err != nil {
		return err
	}

	pluginDir := layout.PluginDir(m.pluginDirectory, pluginName)
	fmt.Printf("Removing plugin from: %s\n", pluginDir)

	err := os.RemoveAll(pluginDir)
	if err != nil {
		return fmt.Errorf("failed to remove plugin directory: %w", err)
	}

	fmt.Println("Cleaning up plugin files...")

	os.Remove(layout.ContractFile(m.pluginDirectory, pluginName))

	fmt.Printf("✓ Plugin '%s' successfully removed!\n", pluginName)

	return nil
}

// RemoveAll deletes every plugin found under the plugins root.
func (m *Manager) RemoveAll() error {
	plugins, err := os.ReadDir(layout.PluginsRoot(m.pluginDirectory))
	if err != nil {
		return fmt.Errorf("failed to read plugins directory: %w", err)
	}

	fmt.Println("Found", len(plugins), "plugins to remove:")

	for _, plugin := range plugins {
		pluginDir := layout.PluginDir(m.pluginDirectory, plugin.Name())
		fmt.Printf("Removing plugin from: %s\n", pluginDir)

		err := os.RemoveAll(pluginDir)
		if err != nil {
			return fmt.Errorf("failed to remove plugin directory: %w", err)
		}

		fmt.Printf("Cleaning up plugin files for '%s'...\n", plugin.Name())

		os.Remove(layout.ContractFile(m.pluginDirectory, plugin.Name()))

		fmt.Printf("✓ Plugin '%s' successfully removed!\n", plugin.Name())
	}

	return nil
}
