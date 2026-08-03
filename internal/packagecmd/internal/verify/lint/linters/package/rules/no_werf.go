package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: reject custom werf configuration from package source roots.

// NoWerfRuleID is the stable identifier used to reference this rule in configuration.
const NoWerfRuleID = "no_werf"

// NoWerfRule checks that root-level werf build files are absent.
type NoWerfRule struct {
	collector *diag.Collector
	path      string
}

// NewNoWerfRule constructs a NoWerfRule scoped to path.
func NewNoWerfRule(path string, collector *diag.Collector) *NoWerfRule {
	return &NoWerfRule{
		path:      path,
		collector: collector.With(diag.RuleID(NoWerfRuleID)),
	}
}

// Check reports when root-level werf build files exist.
func (r *NoWerfRule) Check(_ context.Context) {
	r.checkPath(".werf", ".werf directory found - custom build files allowed only in hooks/ or images/")
	r.checkPath("werf.yaml", "werf.yaml found - custom werf.yaml not allowed")
}

// checkPath reports when name exists in the package root.
func (r *NoWerfRule) checkPath(name, message string) {
	path := filepath.Join(r.path, name)

	if _, err := os.Stat(path); err != nil {
		return
	}

	r.collector.With(diag.Path(path)).Error("%s", message)
}
