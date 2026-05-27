package templates

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates/rules"
)

// LinterID is the stable identifier used to reference this linter in configuration and diagnostics.
const LinterID = "templates"

// Config holds the path and settings required to construct a Linter.
type Config struct {
	Rendered []render.Object
	Settings LinterSettings
}

// LinterSettings combines the shared linter-level severity with the per-rule settings for this linter.
type LinterSettings struct {
	lint.LinterSettings
	RulesSettings
}

// RulesSettings holds the severity configuration for each tunable rule in the templates linter.
// The instance-prefix and instance-namespace rules are intentionally absent: they encode hard
// multi-instance contracts and run at the linter-level severity without per-rule overrides.
type RulesSettings struct {
	PDB         lint.RuleSettings
	ServicePort lint.RuleSettings
	VPA         lint.RuleSettings
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

// Linter runs templates rules against an application package directory.
type Linter struct {
	config   Config
	settings RulesSettings

	collector *diag.Collector
}

// Lint executes all templates rules against the pre-rendered objects.
func (l *Linter) Lint(ctx context.Context) {
	rules.NewInstancePrefixRule(l.config.Rendered, l.collector).Check(ctx)
	rules.NewInstanceNamespaceRule(l.config.Rendered, l.collector).Check(ctx)
	rules.NewPDBRule(l.config.Rendered, l.collector.With(diag.MaxLevel(l.settings.PDB.Impact))).Check(ctx)
	rules.NewServicePortRule(l.config.Rendered, l.collector.With(diag.MaxLevel(l.settings.ServicePort.Impact))).Check(ctx)
	rules.NewVPARule(l.config.Rendered, l.collector.With(diag.MaxLevel(l.settings.VPA.Impact))).Check(ctx)
}
