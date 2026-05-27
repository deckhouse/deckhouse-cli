package rules

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/iconutil"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: cap the rendered width and height of the icon for catalog UI.

// ShapeRuleID is the stable identifier used to reference this rule in configuration.
const ShapeRuleID = "shape"

// maxIconSide caps each side of the rendered icon, in pixels.
const maxIconSide = 300

// ShapeRule reports a finding when the icon's reported width or height
// exceeds maxIconSide. Vector formats whose decoder reports zero dimensions
// are silently skipped — they have no intrinsic rasterized size.
type ShapeRule struct {
	collector *diag.Collector
	icon      iconutil.Icon
}

// NewShapeRule constructs a ShapeRule scoped to icon, tagging diagnostics with the rule ID.
func NewShapeRule(icon iconutil.Icon, collector *diag.Collector) *ShapeRule {
	return &ShapeRule{
		icon:      icon,
		collector: collector.With(diag.RuleID(ShapeRuleID)),
	}
}

// Check reports an error when icon dimensions exceed maxIconSide × maxIconSide.
func (r *ShapeRule) Check(_ context.Context) {
	if r.icon.Shape.Width > maxIconSide || r.icon.Shape.Height > maxIconSide {
		r.collector.Error("icon is %dx%d, exceeds %dx%d",
			r.icon.Shape.Width, r.icon.Shape.Height, maxIconSide, maxIconSide)
	}
}
