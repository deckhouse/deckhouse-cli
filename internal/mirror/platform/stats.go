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

package platform

// ComponentStats is the platform phase's image accounting, mapped into the
// top-level summary by the pull orchestrator.
type ComponentStats struct {
	Attempted bool
	Images    int
	// Versions are the resolved Deckhouse release versions to mirror (e.g.
	// ["v1.69.0"]); Channels are the release channels mapped to them. Both are
	// resolved by findTagsToMirror before any download, so they are populated in
	// dry-run as well.
	Versions []string
	Channels []string
}

// Stats returns image accounting for the platform phase. In dry-run it reports
// planned counts from the download list; otherwise it reports the actual number
// of manifests pulled into the OCI layouts, captured before packing (see
// Service.pulledImages).
func (svc *Service) Stats() ComponentStats {
	if svc.options.DryRun {
		images := len(svc.downloadList.Deckhouse) +
			len(svc.downloadList.DeckhouseInstall) +
			len(svc.downloadList.DeckhouseInstallStandalone) +
			len(svc.downloadList.DeckhouseReleaseChannel)

		return ComponentStats{Attempted: true, Images: images, Versions: svc.resolvedVersions, Channels: svc.resolvedChannels}
	}

	return ComponentStats{Attempted: true, Images: svc.pulledImages, Versions: svc.resolvedVersions, Channels: svc.resolvedChannels}
}
