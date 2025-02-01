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

package bundle

import (
	"fmt"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/v1/layout"
)

func MandatoryLayoutsForPlatform(platformPkgDir string) map[string]string {
	return map[string]string{
		"root layout":             platformPkgDir,
		"installers layout":       filepath.Join(platformPkgDir, "install"),
		"release channels layout": filepath.Join(platformPkgDir, "release-channel"),
	}
}

func MandatoryLayoutsForSecurityDatabase(securityDbPkgDir string) map[string]string {
	return map[string]string{
		"trivy database layout":      filepath.Join(securityDbPkgDir, "trivy-db"),
		"trivy bdu layout":           filepath.Join(securityDbPkgDir, "trivy-bdu"),
		"trivy java database layout": filepath.Join(securityDbPkgDir, "trivy-java-db"),
		"trivy checks layout":        filepath.Join(securityDbPkgDir, "trivy-checks"),
	}
}

func MandatoryLayoutsForModule(modulePkgDir string) map[string]string {
	return map[string]string{
		"module root layout":             filepath.Join(modulePkgDir),
		"module release channels layout": filepath.Join(modulePkgDir, "release"),
	}
}

func ValidateUnpackedPackage(mandatoryLayouts map[string]string) error {
	for layoutDescription, fsPath := range mandatoryLayouts {
		l, err := layout.FromPath(fsPath)
		if err != nil {
			return fmt.Errorf("%s: %w", layoutDescription, err)
		}
		index, err := l.ImageIndex()
		if err != nil {
			return fmt.Errorf("%s image index: %w", layoutDescription, err)
		}

		indexManifest, err := index.IndexManifest()
		if err != nil {
			return fmt.Errorf("%s image index manifest: %w", layoutDescription, err)
		}

		if len(indexManifest.Manifests) == 0 {
			return fmt.Errorf("No images in %s", layoutDescription)
		}
	}

	return nil
}
