package rules

import (
	"context"
	"errors"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/iconutil"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require a package icon at the package root in one of the supported formats.

// IconRuleID is the stable identifier used to reference this rule in configuration.
const IconRuleID = "icon"

// IconRule reports a finding when no supported icon file exists at the package
// root. The rule emits Error; default severity is downgraded to Warn via the
// layout linter's per-rule level configuration (see settings/load.go).
type IconRule struct {
	collector *diag.Collector
	path      string
}

// NewIconRule constructs an IconRule scoped to path, tagging diagnostics with the rule ID.
func NewIconRule(path string, collector *diag.Collector) *IconRule {
	return &IconRule{
		path:      path,
		collector: collector.With(diag.RuleID(IconRuleID)),
	}
}

// Check verifies that an icon file exists at the package root. Layout cares
// only about presence; content/format problems are surfaced by the icon linter.
func (r *IconRule) Check(_ context.Context) {
	_, err := iconutil.Find(r.path)
	if errors.Is(err, iconutil.ErrNoIcon) {
		r.collector.Error("%s is missing in package root", iconutil.Expected())
	}
}
