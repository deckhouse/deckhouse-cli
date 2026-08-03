package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: reject generated Helm ignore files from package source roots.

// NoHelmignoreRuleID is the stable identifier used to reference this rule in configuration.
const NoHelmignoreRuleID = "no_helmignore"

// NoHelmignoreRule checks that .helmignore is absent from the package root.
type NoHelmignoreRule struct {
	collector *diag.Collector
	path      string
}

// NewNoHelmignoreRule constructs a NoHelmignoreRule scoped to path.
func NewNoHelmignoreRule(path string, collector *diag.Collector) *NoHelmignoreRule {
	return &NoHelmignoreRule{
		path:      path,
		collector: collector.With(diag.RuleID(NoHelmignoreRuleID)),
	}
}

// Check reports when .helmignore exists in the package root.
func (r *NoHelmignoreRule) Check(_ context.Context) {
	path := filepath.Join(r.path, ".helmignore")

	if _, err := os.Stat(path); err != nil {
		return
	}

	r.collector.With(diag.Path(path)).
		Error(".helmignore found - file is generated at build time")
}
