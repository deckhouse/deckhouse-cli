package rules

import (
	"context"
	"slices"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/iconutil"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require the icon's extension to be one of the supported formats.

// ExtRuleID is the stable identifier used to reference this rule in configuration.
const ExtRuleID = "ext"

// allowedExts is the closed set of icon extensions accepted by this rule.
// Keep in sync with iconutil's decoder registry — these are the formats whose
// content we can validate end-to-end.
var allowedExts = []string{".png", ".webp", ".jpg", ".jpeg", ".svg"}

// ExtRule reports a finding when the icon's extension is not in allowedExts.
type ExtRule struct {
	collector *diag.Collector
	icon      iconutil.Icon
}

// NewExtRule constructs an ExtRule scoped to icon, tagging diagnostics with the rule ID.
func NewExtRule(icon iconutil.Icon, collector *diag.Collector) *ExtRule {
	return &ExtRule{
		icon:      icon,
		collector: collector.With(diag.RuleID(ExtRuleID)),
	}
}

// Check reports an error when the icon's extension is not supported.
func (r *ExtRule) Check(_ context.Context) {
	if slices.Contains(allowedExts, r.icon.Ext) {
		return
	}

	r.collector.With(diag.Value(r.icon.Ext)).
		Error("icon extension is not supported (allowed: %s)", strings.Join(allowedExts, ", "))
}
