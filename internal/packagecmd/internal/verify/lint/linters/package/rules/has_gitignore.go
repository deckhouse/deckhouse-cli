package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require source-control ignore rules in package source trees.

// HasGitignoreRuleID is the stable identifier used to reference this rule in configuration.
const HasGitignoreRuleID = "has_gitignore"

// HasGitignoreRule checks that .gitignore exists in the package root.
type HasGitignoreRule struct {
	collector *diag.Collector
	path      string
}

// NewHasGitignoreRule constructs a HasGitignoreRule scoped to path.
func NewHasGitignoreRule(path string, collector *diag.Collector) *HasGitignoreRule {
	return &HasGitignoreRule{
		path:      path,
		collector: collector.With(diag.RuleID(HasGitignoreRuleID)),
	}
}

// Check reports when .gitignore is missing or is not a regular file.
func (r *HasGitignoreRule) Check(_ context.Context) {
	path := filepath.Join(r.path, ".gitignore")
	collector := r.collector.With(diag.Path(path))

	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			collector.Warn(".gitignore must be a file in package root")
		}

		return
	}

	if os.IsNotExist(err) {
		collector.Warn(".gitignore file is missing in package root")

		return
	}

	collector.With(diag.Value(err.Error())).Warn("failed to check .gitignore file")
}
