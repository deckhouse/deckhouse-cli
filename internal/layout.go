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

// deckhouse repo structure (relative to root path like registry.deckhouse.io/deckhouse/fe)
//
// Platform:
//
//	<root>:<version>                                    - Deckhouse main image
//	<root>/release-channel:<channel>                    - Release channel metadata
//	<root>/install:<version>                            - Installer image
//	<root>/install-standalone:<version>                 - Standalone installer
//
// Security:
//
//	<root>/security/<security-name>:<version>           - Security databases (trivy-db, trivy-bdu, etc.)
//
// Modules:
//
//	<root>/modules/<module-name>:<version>                    - Module main image
//	<root>/modules/<module-name>/release:<channel>            - Module release channel metadata
//	<root>/modules/<module-name>/extra/<extra-name>:<version> - Module extra images
const (
	InstallSegment           = "install"
	InstallStandaloneSegment = "install-standalone"
	ReleaseChannelSegment    = "release-channel"

	ModulesSegment        = "modules"
	ModulesReleaseSegment = "release"
	ModulesExtraSegment   = "extra"

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

	// Module paths are relative to modules/<module-name>/ directory
	MirrorTypeModules:                "",                    // Module main image at root of module dir
	MirrorTypeModulesReleaseChannels: ModulesReleaseSegment, // modules/<name>/release

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
