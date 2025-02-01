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
