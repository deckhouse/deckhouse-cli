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

package modules

import (
	"sort"
	"strings"

	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// vexTagSuffix marks a manifest as a VEX attestation (cosign ".att" convention).
const vexTagSuffix = ".att"

// ModuleStat is one module's contribution to the pull.
type ModuleStat struct {
	Name   string
	Images int
	// VEX is how many of Images are VEX attestations (a subset of Images, not
	// an additional count).
	VEX int
	// Versions are the resolved module versions to pull, e.g.
	// ["v1.10.3", "v1.9.16"]. Resolved before download, so available in dry-run.
	Versions []string
}

// ModulesStats is the modules phase's accounting, mapped into the top-level
// summary by the pull orchestrator.
type ModulesStats struct {
	Attempted       bool
	OnlyExtraImages bool
	Modules         []ModuleStat
	TotalImages     int
	// TotalVEX is the number of VEX attestations across all modules, a subset of
	// TotalImages.
	TotalVEX int
}

// moduleName is the registry module name (e.g. "code", "csi-nfs") used as the
// key of Service.moduleStats. It is an alias for string, so it interoperates
// with the plain module names used throughout the package without conversions.
type moduleName = string

// modulePullStat is the internal per-module accounting accumulated during a
// pull and keyed by module name in Service.moduleStats; Stats builds the
// exported ModuleStat from it. The fields are filled at different stages:
// versions during resolution (before download), images and vex before packing
// (bundle.Pack deletes the OCI layouts as it tars them, so the counts cannot be
// taken in Stats, which runs afterwards).
type modulePullStat struct {
	// images is the total image manifest count across the module's layouts.
	images int
	// vex is how many of images are VEX attestations (".att" suffix), a subset.
	vex int
	// versions are the versions selected for pull, e.g. ["v1.10.3", "v1.9.16"].
	versions []string
}

// Stats returns accounting for the modules phase. In dry-run it reports planned
// per-module counts from the download lists; otherwise it reports the actual
// number of manifests pulled into each module's OCI layouts, captured before
// packing in Service.moduleStats (modules that produced no images are omitted,
// matching packModules, which emits no tar for them).
func (svc *Service) Stats() ModulesStats {
	stats := ModulesStats{
		Attempted:       true,
		OnlyExtraImages: svc.options.OnlyExtraImages,
	}

	if svc.options.DryRun {
		for name, dl := range svc.modulesDownloadList.list {
			images := len(dl.Module) + len(dl.ModuleReleaseChannels) + len(dl.ModuleExtra)
			stats.Modules = append(stats.Modules, ModuleStat{Name: name, Images: images, Versions: svc.moduleStats[name].versions})
			stats.TotalImages += images
		}
	} else {
		for name, ms := range svc.moduleStats {
			if ms.images == 0 {
				continue
			}

			stats.Modules = append(stats.Modules, ModuleStat{Name: name, Images: ms.images, VEX: ms.vex, Versions: ms.versions})
			stats.TotalImages += ms.images
			stats.TotalVEX += ms.vex
		}
	}

	sort.Slice(stats.Modules, func(i, j int) bool {
		return stats.Modules[i].Name < stats.Modules[j].Name
	})

	return stats
}

// capturePulledImages records, per module, the image and VEX manifest counts
// pulled into its OCI layouts, merging them into the existing moduleStats entry
// (which already holds the resolved versions). It must run before packing
// deletes the layout files (see bundle.Pack).
func (svc *Service) capturePulledImages(modules []moduleData) {
	if svc.layout == nil {
		return
	}

	for _, module := range modules {
		ml := svc.layout.Module(module.name)
		if ml == nil {
			continue
		}

		paths := ml.AsList()

		stat := svc.moduleStats[module.name]
		stat.images = regimage.CountManifests(paths)
		// VEX attestations live in the same layouts as regular images and are
		// recognised by the ".att" suffix on their short-tag annotation; count
		// them as a subset of the total.
		stat.vex = regimage.CountManifestsMatching(paths,
			func(annotations map[string]string) bool {
				return strings.HasSuffix(annotations[regimage.AnnotationImageShortTag], vexTagSuffix)
			})
		svc.moduleStats[module.name] = stat
	}
}
