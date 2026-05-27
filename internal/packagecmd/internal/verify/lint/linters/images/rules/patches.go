package rules

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require image patch files to live in patches directories with matching README documentation.

// PatchesRuleID is the stable identifier used to reference this rule in configuration.
const PatchesRuleID = "patches"

const (
	patchExt            = ".patch"
	readmeFileName      = "README.md"
	readmeHeadingPrefix = "# "
	imagesDirName       = "images"
)

var (
	patchFilePattern = regexp.MustCompile(`^\d{3}-.*\.patch$`)
	patchDirPattern  = regexp.MustCompile(`^images/[\w/\-.]*/patches.*$`)
)

// PatchesRule validates image patch file placement, naming, and README references.
type PatchesRule struct {
	collector *diag.Collector
	path      string
}

// NewPatchesRule constructs a PatchesRule scoped to path, tagging diagnostics with the rule ID.
func NewPatchesRule(path string, res *diag.Collector) *PatchesRule {
	return &PatchesRule{
		path:      path,
		collector: res.With(diag.RuleID(PatchesRuleID)),
	}
}

// Check verifies all .patch files found under the package directory.
func (r *PatchesRule) Check(_ context.Context) {
	imagesDir := filepath.Join(r.path, imagesDirName)
	if _, err := os.Stat(imagesDir); os.IsNotExist(err) {
		return
	}

	files, err := collectPatchFiles(r.path)
	if err != nil {
		r.collector.With(
			diag.Path(imagesDir),
			diag.Value(err.Error())).
			Error("cannot scan package patches")

		return
	}

	// readmeContents caches README.md content per directory (absent key = dir not yet seen).
	// A nil value means the README was missing or unreadable.
	readmeContents := make(map[string][]byte)

	for _, file := range files {
		dir := filepath.Dir(file)
		relFile := packageRelativePath(r.path, file)

		if _, seen := readmeContents[dir]; !seen {
			relDir := packageRelativePath(r.path, dir)

			if !patchDirPattern.MatchString(relDir) {
				r.collector.With(diag.Path(relDir)).Error("patch file should be in images/<image_name>/patches/ directory")
			}

			content, readErr := os.ReadFile(filepath.Join(dir, readmeFileName))
			switch {
			case os.IsNotExist(readErr):
				r.collector.With(diag.Path(relDir)).Error("patch file should have a corresponding README file")

				readmeContents[dir] = nil
			case readErr != nil:
				r.collector.With(diag.Path(relDir)).Error("error reading README.md file: %s", readErr.Error())

				readmeContents[dir] = nil
			default:
				readmeContents[dir] = content
			}
		}

		if !patchFilePattern.MatchString(filepath.Base(file)) {
			r.collector.With(diag.Path(relFile)).Error("patch file name should match pattern XXX-<patch-name>.patch")
		}

		if content := readmeContents[dir]; content != nil {
			heading := readmeHeadingPrefix + filepath.Base(file)
			if !strings.Contains(string(content), heading) {
				r.collector.With(diag.Path(relFile)).Error("README.md file does not contain %s", heading)
			}
		}
	}
}

// collectPatchFiles returns all .patch files below root in stable order.
func collectPatchFiles(root string) ([]string, error) {
	var files []string

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if !d.IsDir() && filepath.Ext(d.Name()) == patchExt {
			files = append(files, path)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.Sort(files)

	return files, nil
}

// packageRelativePath returns fullPath relative to packagePath using slash separators.
func packageRelativePath(packagePath, fullPath string) string {
	relPath, err := filepath.Rel(packagePath, fullPath)
	if err != nil {
		return fullPath
	}

	return filepath.ToSlash(relPath)
}
