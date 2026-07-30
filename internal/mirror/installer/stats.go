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

package installer

// ComponentStats is the installer phase's image accounting, mapped into the
// top-level summary by the pull orchestrator.
type ComponentStats struct {
	Attempted bool
	Images    int
	// Tag is the installer image tag mirrored ("latest" by default, or a pinned
	// --installer-tag). Resolved before download, so it is set in dry-run too.
	Tag string
}

// Stats returns image accounting for the installer phase. In dry-run it reports
// the planned count from the download list; otherwise it reports the actual
// number of manifests pulled into the OCI layout, captured before packing (see
// Service.pulledImages).
//
// When PullInstaller gracefully skipped the phase on an access error (e.g. the
// installer repo returns 404), the phase is reported as not attempted so the
// summary renders "not pulled" rather than falsely claiming the tag was
// mirrored. This applies to dry-run as well: a skipped phase must not show up
// in the plan.
func (svc *Service) Stats() ComponentStats {
	tag := defaultTargetTag
	if svc.options.TargetTag != "" {
		tag = svc.options.TargetTag
	}

	if svc.accessSkipped {
		return ComponentStats{Attempted: false, Tag: tag}
	}

	if svc.options.DryRun {
		return ComponentStats{Attempted: true, Images: len(svc.downloadList.Installer), Tag: tag}
	}

	return ComponentStats{Attempted: true, Images: svc.pulledImages, Tag: tag}
}
