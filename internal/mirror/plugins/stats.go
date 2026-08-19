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
	"github.com/google/go-containerregistry/pkg/v1/layout"

	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// PluginVersionStat is one pulled plugin version with its provenance, ready
// for the summary's per-module grouping.
type PluginVersionStat struct {
	Version string
	Reasons []Reason
}

// PluginStat is one plugin's contribution to the pull.
type PluginStat struct {
	Name   string
	Images int
	// Versions are the pulled versions, newest first. Filled at resolution
	// time, so available in dry-run too.
	Versions []PluginVersionStat
}

// PluginsStats is the plugins phase's accounting, mapped into the top-level
// summary by the pull orchestrator.
type PluginsStats struct {
	Attempted   bool
	Plugins     []PluginStat
	Skipped     []SkippedPlugin
	Warnings    []string
	TotalImages int
}

// pluginsPullStats is the internal accumulator behind Stats. The resolution
// is recorded up front (dry-run friendly); image counts are captured before
// packing deletes the layouts (see bundle.Pack).
type pluginsPullStats struct {
	attempted      bool
	resolution     *Resolution
	imagesByPlugin map[pluginName]int
}

func newPluginsPullStats() *pluginsPullStats {
	return &pluginsPullStats{
		imagesByPlugin: make(map[pluginName]int),
	}
}

func (s *pluginsPullStats) recordResolution(resolution *Resolution) {
	s.resolution = resolution
}

// captureImages records per-plugin manifest counts from the OCI layouts. It
// must run before packing deletes the layout files.
func (s *pluginsPullStats) captureImages(layouts map[pluginName]*regimage.ImageLayout) {
	for name, pluginLayout := range layouts {
		s.imagesByPlugin[name] = regimage.CountManifests([]layout.Path{pluginLayout.Path()})
	}
}

// Stats returns accounting for the plugins phase.
func (svc *Service) Stats() PluginsStats {
	stats := PluginsStats{Attempted: svc.stats.attempted}

	resolution := svc.stats.resolution
	if resolution == nil {
		return stats
	}

	for _, plugin := range resolution.Plugins {
		versions := make([]PluginVersionStat, 0, len(plugin.Versions))
		for _, sv := range plugin.Versions {
			versions = append(versions, PluginVersionStat{Version: sv.Version.Original(), Reasons: sv.Reasons})
		}

		images := svc.stats.imagesByPlugin[plugin.Name]

		stats.Plugins = append(stats.Plugins, PluginStat{Name: plugin.Name, Images: images, Versions: versions})
		stats.TotalImages += images
	}

	stats.Skipped = resolution.Skipped
	stats.Warnings = resolution.Warnings

	return stats
}
