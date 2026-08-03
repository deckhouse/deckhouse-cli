package rules

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/iconutil"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require an icon file at the package docs.

// HasIconRuleID is the stable identifier used to reference this rule in configuration.
const HasIconRuleID = "has_icon"

// HasIconRule checks that a supported icon file exists at the package docs.
type HasIconRule struct {
	collector *diag.Collector
	path      string
}

// NewHasIconRule constructs a HasIconRule scoped to path.
func NewHasIconRule(path string, collector *diag.Collector) *HasIconRule {
	return &HasIconRule{
		path:      path,
		collector: collector.With(diag.RuleID(HasIconRuleID)),
	}
}

// Check reports when no supported icon file exists at the package docs.
func (r *HasIconRule) Check(_ context.Context) {
	path := filepath.Join(r.path, "docs")
	collector := r.collector.With(diag.Path(path))

	if _, err := iconutil.Find(r.path); errors.Is(err, iconutil.ErrNoIcon) {
		collector.Warn("%s is missing in package docs", iconutil.Expected())
	}
}
