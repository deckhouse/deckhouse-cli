package bundle

import (
	"fmt"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
)

func ValidateUnpackedBundle(mirrorCtx *contexts.PushContext) error {
	mandatoryLayouts := map[string]string{
		"root layout":                mirrorCtx.UnpackedImagesPath,
		"installers layout":          filepath.Join(mirrorCtx.UnpackedImagesPath, "install"),
		"release channels layout":    filepath.Join(mirrorCtx.UnpackedImagesPath, "release-channel"),
		"trivy database layout":      filepath.Join(mirrorCtx.UnpackedImagesPath, "security", "trivy-db"),
		"trivy bdu layout":           filepath.Join(mirrorCtx.UnpackedImagesPath, "security", "trivy-bdu"),
		"trivy java database layout": filepath.Join(mirrorCtx.UnpackedImagesPath, "security", "trivy-java-db"),
	}

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
