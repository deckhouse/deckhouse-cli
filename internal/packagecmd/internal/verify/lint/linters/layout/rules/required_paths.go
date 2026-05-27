package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// requiredRootPathsRule checks that configured files and directories exist in the package root.
type requiredRootPathsRule struct {
	collector *diag.Collector
	path      string
	files     []string
	dirs      []string
}

// newRequiredRootPathsRule constructs a reusable presence check for root-level files and directories.
func newRequiredRootPathsRule(path string, res *diag.Collector, files, dirs []string) *requiredRootPathsRule {
	return &requiredRootPathsRule{
		path:      path,
		collector: res,
		files:     files,
		dirs:      dirs,
	}
}

// Check verifies that every configured file and directory exists in the package root.
func (r *requiredRootPathsRule) Check(_ context.Context) {
	for _, file := range r.files {
		r.checkFile(file)
	}

	for _, dir := range r.dirs {
		r.checkDir(dir)
	}
}

// checkFile reports a finding when name does not exist as a regular file.
func (r *requiredRootPathsRule) checkFile(name string) {
	path := filepath.Join(r.path, name)

	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			r.collector.With(diag.Path(path)).Error("%s must be a file in package root", name)
		}

		return
	}

	if os.IsNotExist(err) {
		r.collector.With(diag.Path(path)).Error("%s file is missing in package root", name)
		return
	}

	r.collector.With(
		diag.Path(path),
		diag.Value(err.Error())).
		Error("failed to check %s file", name)
}

// checkDir reports a finding when name does not exist as a directory.
func (r *requiredRootPathsRule) checkDir(name string) {
	path := filepath.Join(r.path, name)

	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			r.collector.With(diag.Path(path)).Error("%s must be a directory in package root", name)
		}

		return
	}

	if os.IsNotExist(err) {
		r.collector.With(diag.Path(path)).Error("%s directory is missing in package root", name)
		return
	}

	r.collector.With(
		diag.Path(path),
		diag.Value(err.Error())).
		Error("failed to check %s directory", name)
}
