package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require documentation in every package artifact.

// HasDocsRuleID is the stable identifier used to reference this rule in configuration.
const HasDocsRuleID = "has_docs"

// HasDocsRule checks that docs/ exists in the package root.
type HasDocsRule struct {
	collector *diag.Collector
	path      string
}

// NewHasDocsRule constructs a HasDocsRule scoped to path.
func NewHasDocsRule(path string, collector *diag.Collector) *HasDocsRule {
	return &HasDocsRule{
		path:      path,
		collector: collector.With(diag.RuleID(HasDocsRuleID)),
	}
}

// Check reports when docs/ is missing or is not a directory.
func (r *HasDocsRule) Check(_ context.Context) {
	path := filepath.Join(r.path, "docs")
	collector := r.collector.With(diag.Path(path))

	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			collector.Error("docs must be a directory in package root")
		}

		return
	}

	if os.IsNotExist(err) {
		collector.Error("docs directory is missing in package root")

		return
	}

	collector.With(diag.Value(err.Error())).Error("failed to check docs directory")
}
