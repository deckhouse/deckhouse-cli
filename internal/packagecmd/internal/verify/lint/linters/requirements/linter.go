package requirements

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/requirements/rules"
)

// LinterID is the stable identifier used to reference this linter in configuration and diagnostics.
const LinterID = "requirements"

// Config holds the package definition and settings required to construct a Linter.
type Config struct {
	Definition packages.Definition
	Settings   LinterSettings
}

// LinterSettings combines the shared linter-level severity with the per-rule settings for this linter.
type LinterSettings struct {
	lint.LinterSettings
	RulesSettings
}

// RulesSettings holds the severity configuration for each rule in the requirements linter.
type RulesSettings struct {
	ModuleGroups lint.RuleSettings
	Constraints  lint.RuleSettings
}

// NewLinter constructs a Linter from cfg, scoping its diagnostics to this linter and capping severity at the configured level.
func NewLinter(cfg Config, res *diag.Collector) *Linter {
	return &Linter{
		settings: cfg.Settings.RulesSettings,
		config:   cfg,
		collector: res.With(
			diag.LinterID(LinterID),
			diag.MaxLevel(cfg.Settings.LinterSettings.Impact)),
	}
}

// Linter checks that the requirements declared in package.yaml are well-formed.
// Only the shape is checked: whether the cluster actually satisfies them is
// decided by deckhouse-controller at install time, and no cluster exists here.
type Linter struct {
	config   Config
	settings RulesSettings

	collector *diag.Collector
}

// Lint executes all requirements rules against the configured package definition.
func (l *Linter) Lint(ctx context.Context) {
	reqs := l.config.Definition.Requirements

	rules.NewModuleGroupsRule(reqs.Modules,
		l.collector.With(diag.MaxLevel(l.settings.ModuleGroups.Impact))).Check(ctx)
	rules.NewConstraintsRule(reqs,
		l.collector.With(diag.MaxLevel(l.settings.Constraints.Impact))).Check(ctx)
}
