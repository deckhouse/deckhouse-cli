package layout

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/layout/rules"
)

// LinterID is the stable identifier used to reference this linter in configuration and diagnostics.
const LinterID = "layout"

// Config holds the path and settings required to construct a Linter.
type Config struct {
	Path     string
	Settings LinterSettings
}

// LinterSettings combines the shared linter-level severity with the per-rule settings for this linter.
type LinterSettings struct {
	lint.LinterSettings
	RulesSettings
}

// RulesSettings holds the severity configuration for each rule in the layout linter.
type RulesSettings struct {
	NoWerf       lint.RuleSettings
	NoChart      lint.RuleSettings
	NoHelmignore lint.RuleSettings
	Gitignore    lint.RuleSettings
	Changelog    lint.RuleSettings
	Docs         lint.RuleSettings
	Icon         lint.RuleSettings
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

// Linter runs layout rules against an application package directory.
type Linter struct {
	config   Config
	settings RulesSettings

	collector *diag.Collector
}

// Lint executes all layout rules against the configured package path.
func (l *Linter) Lint(ctx context.Context) {
	rules.NewNoWerfRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.NoWerf.Impact))).Check(ctx)
	rules.NewNoChartRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.NoChart.Impact))).Check(ctx)
	rules.NewNoHelmignoreRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.NoHelmignore.Impact))).Check(ctx)
	rules.NewGitignoreRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.Gitignore.Impact))).Check(ctx)
	rules.NewChangelogRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.Changelog.Impact))).Check(ctx)
	rules.NewDocsRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.Docs.Impact))).Check(ctx)
	rules.NewIconRule(l.config.Path, l.collector.With(diag.MaxLevel(l.settings.Icon.Impact))).Check(ctx)
}
