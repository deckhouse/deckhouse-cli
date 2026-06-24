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

package plugins

import (
	"context"
	"fmt"

	"github.com/Masterminds/semver/v3"
)

// PublishedVersions lists the plugin's published tags and returns them as
// semver versions, newest first (unparseable tags are dropped).
func (m *Manager) PublishedVersions(ctx context.Context, pluginName string) ([]*semver.Version, error) {
	tags, err := m.service.ListPluginTags(ctx, pluginName)
	if err != nil {
		return nil, fmt.Errorf("failed to list plugin tags: %w", err)
	}

	return sortedSemverDesc(tags), nil
}

// InstalledVersionOrNil returns the active installed version of the
// plugin, or nil when the plugin is not installed or its version cannot be
// probed - best-effort: a version listing then simply carries no "current" marker.
func (m *Manager) InstalledVersionOrNil(pluginName string) *semver.Version {
	if installed, _ := m.checkInstalled(pluginName); !installed {
		return nil
	}

	current, err := m.getInstalledPluginVersion(pluginName)
	if err != nil {
		return nil
	}

	return current
}
