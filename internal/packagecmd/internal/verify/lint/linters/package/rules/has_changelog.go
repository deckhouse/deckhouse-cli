package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require release history in every package artifact.

// HasChangelogRuleID is the stable identifier used to reference this rule in configuration.
const HasChangelogRuleID = "has_changelog"

// HasChangelogRule checks that changelog.yaml exists in the package root.
type HasChangelogRule struct {
	collector *diag.Collector
	path      string
}

// NewHasChangelogRule constructs a HasChangelogRule scoped to path.
func NewHasChangelogRule(path string, collector *diag.Collector) *HasChangelogRule {
	return &HasChangelogRule{
		path:      path,
		collector: collector.With(diag.RuleID(HasChangelogRuleID)),
	}
}

// Check reports when changelog.yaml is missing or is not a regular file.
func (r *HasChangelogRule) Check(_ context.Context) {
	path := filepath.Join(r.path, "changelog.yaml")
	collector := r.collector.With(diag.Path(path))

	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			collector.Error("changelog.yaml must be a file in package root")
		}

		return
	}

	if os.IsNotExist(err) {
		collector.Error("changelog.yaml file is missing in package root")

		return
	}

	collector.With(diag.Value(err.Error())).Error("failed to check changelog.yaml file")
}
