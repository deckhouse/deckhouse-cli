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

// Scopes lists the verification targets this linter is processed in. It inspects rendered
// Kubernetes manifests, so it is limited to the sources that carry templates/ and charts/;
// the release image ships metadata only and has nothing to render.
var Scopes = lint.EveryType(lint.ScopeStatic, lint.ScopeBundle)

// ruleScopes narrows the rules that encode the multi-instance contract. A name prefixed
// with the instance and a per-instance namespace are properties of application packages,
// which deploy once per instance; modules deploy once per cluster.
var ruleScopes = lint.RuleScopes{
	rules.InstancePrefixRuleID: {
		lint.TypeApplication: {lint.ScopeStatic, lint.ScopeBundle},
	},
	rules.InstanceNamespaceRuleID: {
		lint.TypeApplication: {lint.ScopeStatic, lint.ScopeBundle},
	},
}

// RuleScopes returns the rules narrowed to fewer targets than the linter itself runs in.
// It lets `package doc` report where a rule is processed without restating the table.
func RuleScopes() lint.RuleScopes {
	return ruleScopes
}

// Config holds the rendered objects, target and settings required to construct a Linter.
type Config struct {
	Rendered []render.Object
	Target   lint.Target
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

// Lint executes the templates rules that apply to the configured target against the
// pre-rendered objects.
func (l *Linter) Lint(ctx context.Context) {
	if l.runs(rules.InstancePrefixRuleID) {
		rules.NewInstancePrefixRule(l.config.Rendered, l.collector).Check(ctx)
	}

	if l.runs(rules.InstanceNamespaceRuleID) {
		rules.NewInstanceNamespaceRule(l.config.Rendered, l.collector).Check(ctx)
	}

	if l.runs(rules.PDBRuleID) {
		rules.NewPDBRule(l.config.Rendered, l.collector.With(diag.MaxLevel(l.settings.PDB.Impact))).Check(ctx)
	}

	if l.runs(rules.ServicePortRuleID) {
		rules.NewServicePortRule(l.config.Rendered, l.collector.With(diag.MaxLevel(l.settings.ServicePort.Impact))).Check(ctx)
	}

	if l.runs(rules.VPARuleID) {
		rules.NewVPARule(l.config.Rendered, l.collector.With(diag.MaxLevel(l.settings.VPA.Impact))).Check(ctx)
	}
}

// runs reports whether ruleID is processed for the target this linter was built for.
func (l *Linter) runs(ruleID string) bool {
	return ruleScopes.Runs(ruleID, Scopes, l.config.Target)
}
