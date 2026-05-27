package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require a non-empty docs/README.md as the package documentation entry point.

// ReadmeRuleID is the stable identifier used to reference this rule in configuration.
const ReadmeRuleID = "readme"

// ReadmeRule enforces that docs/README.md exists and is not empty.
type ReadmeRule struct {
	collector *diag.Collector
	path      string
}

// NewReadmeRule constructs a ReadmeRule scoped to path, tagging diagnostics with the rule ID.
func NewReadmeRule(path string, res *diag.Collector) *ReadmeRule {
	return &ReadmeRule{
		path:      path,
		collector: res.With(diag.RuleID(ReadmeRuleID)),
	}
}

// Check verifies that docs/README.md exists and has content.
func (r *ReadmeRule) Check(_ context.Context) {
	path := filepath.Join(r.path, "docs", "README.md")

	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		r.collector.With(diag.Path(path)).Error("README.md file is missing in docs/ directory")
		return
	}

	if err != nil {
		r.collector.With(
			diag.Path(path),
			diag.Value(err.Error())).
			Error("failed to check README.md file")

		return
	}

	if info.Size() == 0 {
		r.collector.With(diag.Path(path)).Error("README.md file is empty")
	}
}
