package rules

import (
	"context"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: warn about package-level Helm chart metadata because packages use package.yaml as the root descriptor.

// chartFile is the Helm chart metadata file that should not exist in the package root.
const chartFile = "Chart.yaml"

// NoChartRuleID is the stable identifier used to reference this rule in configuration.
const NoChartRuleID = "no-chart"

// NoChartRule reports Helm chart metadata in the package root.
type NoChartRule struct {
	collector *diag.Collector
	path      string
}

// NewNoChartRule constructs a NoChartRule scoped to path, tagging diagnostics with the rule ID.
func NewNoChartRule(path string, collector *diag.Collector) *NoChartRule {
	return &NoChartRule{
		path:      path,
		collector: collector.With(diag.RuleID(NoChartRuleID)),
	}
}

// Check runs the Chart.yaml check against the package directory.
func (r *NoChartRule) Check(_ context.Context) {
	r.checkChartFile()
}

// checkChartFile reports a finding if Chart.yaml is present.
func (r *NoChartRule) checkChartFile() {
	path := filepath.Join(r.path, chartFile)

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return
	}

	if err != nil {
		return
	}

	r.collector.With(diag.Path(path)).Error("Chart.yaml found - package root metadata must be defined in package.yaml")
}
