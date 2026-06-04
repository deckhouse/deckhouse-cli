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

package packages

import (
	"sort"
	"strings"

	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// vexTagSuffix marks a manifest as a VEX attestation (cosign ".att" convention).
const vexTagSuffix = ".att"

// PackageStat is one package's contribution to the pull.
type PackageStat struct {
	Name   string
	Images int
	// VEX is how many of Images are VEX attestations (a subset of Images, not
	// an additional count).
	VEX int
	// Versions are the resolved package versions to pull, e.g.
	// ["v1.45.2", "v1.44.0"]. Resolved before download, so available in dry-run.
	Versions []string
}

// PackagesStats is the packages phase's accounting, mapped into the top-level
// summary by the pull orchestrator.
type PackagesStats struct {
	Attempted       bool
	OnlyExtraImages bool
	Packages        []PackageStat
	TotalImages     int
	// TotalVEX is the number of VEX attestations across all packages, a subset of
	// TotalImages.
	TotalVEX int
}

// packageName is the registry package name (e.g. "console") used as the key of
// Service.packageStats. It is an alias for string, so it interoperates with the
// plain package names used throughout the package without conversions.
type packageName = string

// packagePullStat is the internal per-package accounting accumulated during a
// pull and keyed by package name in Service.packageStats; Stats builds the
// exported PackageStat from it. The fields are filled at different stages:
// versions during resolution (before download), images and vex before packing
// (bundle.Pack deletes the OCI layouts as it tars them, so the counts cannot be
// taken in Stats, which runs afterwards).
type packagePullStat struct {
	// images is the total image manifest count across the package's layouts.
	images int
	// vex is how many of images are VEX attestations (".att" suffix), a subset.
	vex int
	// versions are the versions selected for pull, e.g. ["v1.45.2", "v1.44.0"].
	versions []string
}

// Stats returns accounting for the packages phase. In dry-run it reports planned
// per-package counts from the download lists; otherwise it reports the actual
// number of manifests pulled into each package's OCI layouts, captured before
// packing in Service.packageStats (packages that produced no images are omitted,
// matching packPackages, which emits no tar for them).
func (svc *Service) Stats() PackagesStats {
	stats := PackagesStats{
		Attempted:       true,
		OnlyExtraImages: svc.options.OnlyExtraImages,
	}

	if svc.options.DryRun {
		for name, dl := range svc.packagesDownloadList.list {
			images := len(dl.Package) + len(dl.PackageVersionChannels) + len(dl.PackageExtra)
			stats.Packages = append(stats.Packages, PackageStat{Name: name, Images: images, Versions: svc.packageStats[name].versions})
			stats.TotalImages += images
		}
	} else {
		for name, ps := range svc.packageStats {
			if ps.images == 0 {
				continue
			}

			stats.Packages = append(stats.Packages, PackageStat{Name: name, Images: ps.images, VEX: ps.vex, Versions: ps.versions})
			stats.TotalImages += ps.images
			stats.TotalVEX += ps.vex
		}
	}

	sort.Slice(stats.Packages, func(i, j int) bool {
		return stats.Packages[i].Name < stats.Packages[j].Name
	})

	return stats
}

// capturePulledImages records, per package, the image and VEX manifest counts
// pulled into its OCI layouts, merging them into the existing packageStats entry
// (which already holds the resolved versions). It must run before packing
// deletes the layout files (see bundle.Pack).
func (svc *Service) capturePulledImages(pkgs []packageData) {
	if svc.layout == nil {
		return
	}

	for _, pkg := range pkgs {
		pl := svc.layout.Package(pkg.name)
		if pl == nil {
			continue
		}

		paths := pl.AsList()

		stat := svc.packageStats[pkg.name]
		stat.images = regimage.CountManifests(paths)
		// VEX attestations live in the same layouts as regular images and are
		// recognised by the ".att" suffix on their short-tag annotation; count
		// them as a subset of the total.
		stat.vex = regimage.CountManifestsMatching(paths,
			func(annotations map[string]string) bool {
				return strings.HasSuffix(annotations[regimage.AnnotationImageShortTag], vexTagSuffix)
			})
		svc.packageStats[pkg.name] = stat
	}
}
