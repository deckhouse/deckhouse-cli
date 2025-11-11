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

package internal

import "path"

// deckhouse repo structure
// root-segment:<version>
// root-segment/install:<version>
// root-segment/install-standalone:<version>
// root-segment/release-channel:<version>
// root-segment/modules/<module-name>:<version>
// root-segment/modules/<module-name>/releases:<version>
// root-segment/modules/<module-name>/extra/<module-extra-name>:<version>
const (
	InstallSegment           = "install"
	InstallStandaloneSegment = "install-standalone"
	ReleaseChannelSegment    = "release-channel"

	ModulesSegment         = "modules"
	ModulesExtraSegment    = "extra"
	ModulesReleasesSegment = "releases"

	SecuritySegment = "security"

	SecurityTrivyDBSegment     = "trivy-db"
	SecurityTrivyBDUSegment    = "trivy-bdu"
	SecurityTrivyJavaDBSegment = "trivy-java-db"
	SecurityTrivyChecksSegment = "trivy-checks"
)

var pathByMirrorType = map[MirrorType]string{
	MirrorTypeDeckhouse:                  "",
	MirrorTypeDeckhouseInstall:           InstallSegment,
	MirrorTypeDeckhouseInstallStandalone: InstallStandaloneSegment,
	MirrorTypeDeckhouseReleaseChannels:   ReleaseChannelSegment,

	MirrorTypeModules:                ModulesSegment,
	MirrorTypeModulesReleaseChannels: ModulesReleasesSegment,
	MirrorTypeModulesExtra:           ModulesExtraSegment,

	MirrorTypeSecurity:                   SecuritySegment,
	MirrorTypeSecurityTrivyDBSegment:     path.Join(SecuritySegment, SecurityTrivyDBSegment),
	MirrorTypeSecurityTrivyBDUSegment:    path.Join(SecuritySegment, SecurityTrivyBDUSegment),
	MirrorTypeSecurityTrivyJavaDBSegment: path.Join(SecuritySegment, SecurityTrivyJavaDBSegment),
	MirrorTypeSecurityTrivyChecksSegment: path.Join(SecuritySegment, SecurityTrivyChecksSegment),
}

// InstallPathByMirrorType returns the path segment for install based on the mirror type.
func InstallPathByMirrorType(mirrorType MirrorType) string {
	return pathByMirrorType[mirrorType]
}
