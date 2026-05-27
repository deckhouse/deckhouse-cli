package rules

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/iconutil"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: cap the icon file size on disk so the artifact stays small in the registry.

// SizeRuleID is the stable identifier used to reference this rule in configuration.
const SizeRuleID = "size"

// maxIconBytes is the per-icon file-size cap.
const maxIconBytes = 150 * 1024

// SizeRule reports a finding when the icon file size exceeds maxIconBytes.
// It consumes the size already captured on Icon — no IO of its own.
type SizeRule struct {
	collector *diag.Collector
	icon      iconutil.Icon
}

// NewSizeRule constructs a SizeRule scoped to icon, tagging diagnostics with the rule ID.
func NewSizeRule(icon iconutil.Icon, collector *diag.Collector) *SizeRule {
	return &SizeRule{
		icon:      icon,
		collector: collector.With(diag.RuleID(SizeRuleID)),
	}
}

// Check reports an error when the icon file size exceeds maxIconBytes.
func (r *SizeRule) Check(_ context.Context) {
	if r.icon.Size > maxIconBytes {
		r.collector.With(diag.Value(r.icon.Size)).
			Error("icon exceeds %d KB limit", maxIconBytes/1024)
	}
}
