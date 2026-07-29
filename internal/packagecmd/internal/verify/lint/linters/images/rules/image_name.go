package rules

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: forbid underscores in image names — both the images/<name> directory
// names and the image: fields declared in images/<name>/werf.inc.yaml. werf builds each
// image as "images/<name>", so an underscore there is not a valid image name.

// ImageNameRuleID is the stable identifier used to reference this rule in configuration.
const ImageNameRuleID = "image-name"

const (
	dockerfileName  = "Dockerfile"
	werfIncFileName = "werf.inc.yaml"
)

// imageDeclPattern captures the value of a top-level image: declaration in a werf.inc.yaml file.
var imageDeclPattern = regexp.MustCompile(`(?m)^image:[ \t]+(.+?)[ \t]*$`)

// ImageNameRule rejects underscores in package image names.
type ImageNameRule struct {
	collector *diag.Collector
	path      string
}

// NewImageNameRule constructs an ImageNameRule scoped to path, tagging diagnostics with the rule ID.
func NewImageNameRule(path string, res *diag.Collector) *ImageNameRule {
	return &ImageNameRule{
		path:      path,
		collector: res.With(diag.RuleID(ImageNameRuleID)),
	}
}

// Check reports underscores in image directory names and in werf.inc.yaml image declarations.
func (r *ImageNameRule) Check(_ context.Context) {
	imagesDir := filepath.Join(r.path, imagesDirName)

	entries, err := os.ReadDir(imagesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}

		r.collector.With(
			diag.Path(imagesDirName),
			diag.Value(err.Error())).
			Error("cannot scan package images")

		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		imageDir := filepath.Join(imagesDir, entry.Name())
		werfInc := filepath.Join(imageDir, werfIncFileName)

		hasWerfInc := isFile(werfInc)
		if !hasWerfInc && !isFile(filepath.Join(imageDir, dockerfileName)) {
			// Not a build image directory: no Dockerfile and no werf.inc.yaml.
			continue
		}

		if strings.Contains(entry.Name(), "_") {
			r.collector.With(
				diag.Path(packageRelativePath(r.path, imageDir)),
				diag.Value(entry.Name())).
				Error("image name %q must not contain underscores", entry.Name())
		}

		if hasWerfInc {
			r.checkWerfInc(werfInc)
		}
	}
}

// checkWerfInc reports underscores in every top-level image: declaration of a werf.inc.yaml file.
func (r *ImageNameRule) checkWerfInc(werfInc string) {
	content, err := os.ReadFile(werfInc)
	if err != nil {
		return
	}

	rel := packageRelativePath(r.path, werfInc)

	for _, match := range imageDeclPattern.FindAllStringSubmatch(string(content), -1) {
		image := match[1]
		if strings.Contains(image, "_") {
			r.collector.With(
				diag.Path(rel),
				diag.Value(image)).
				Error("image name %q in werf.inc.yaml must not contain underscores", image)
		}
	}
}

// isFile reports whether path exists and is a regular file.
func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
