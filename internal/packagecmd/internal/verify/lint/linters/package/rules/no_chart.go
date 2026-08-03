package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: reject Helm chart metadata from package source roots.

// NoChartRuleID is the stable identifier used to reference this rule in configuration.
const NoChartRuleID = "no_chart"

// NoChartRule checks that Chart.yaml is absent from the package root.
type NoChartRule struct {
	collector *diag.Collector
	path      string
}

// NewNoChartRule constructs a NoChartRule scoped to path.
func NewNoChartRule(path string, collector *diag.Collector) *NoChartRule {
	return &NoChartRule{
		path:      path,
		collector: collector.With(diag.RuleID(NoChartRuleID)),
	}
}

// Check reports when Chart.yaml exists in the package root.
func (r *NoChartRule) Check(_ context.Context) {
	path := filepath.Join(r.path, "Chart.yaml")

	if _, err := os.Stat(path); err != nil {
		return
	}

	r.collector.With(diag.Path(path)).
		Warn("Chart.yaml found - package root metadata must be defined in package.yaml")
}
