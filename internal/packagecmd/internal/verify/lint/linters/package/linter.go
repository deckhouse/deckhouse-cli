// Package pkglint holds the linter that inspects the package as an artifact: the shape
// of its root, and the metadata that describes it. Its rules answer questions about the
// package itself rather than about any one thing inside it — file contents are the
// concern of the docs, icon, images and oss linters.
//
// The Go package cannot be named for the linter it implements, because `package` is a
// keyword. The directory is, so the linters tree still maps one-to-one onto the linter
// IDs users type in .pkglint.yaml.
package pkglint

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/package/rules"
)

// LinterID is the stable identifier used to reference this linter in configuration and diagnostics.
const LinterID = "package"

// Scopes lists the verification targets this linter is processed in. The package is
// meaningful for every source and every package type: each one is expected to carry
// docs, a changelog and an icon.
var Scopes = lint.EveryType(lint.AllScopes...)

// ruleScopes narrows the rules that are processed in fewer targets than the linter itself.
var ruleScopes = lint.RuleScopes{
	rules.NoWerfRuleID:       lint.EveryType(lint.ScopeStatic),
	rules.NoChartRuleID:      lint.EveryType(lint.ScopeStatic),
	rules.NoHelmignoreRuleID: lint.EveryType(lint.ScopeStatic),
	rules.HasGitignoreRuleID: lint.EveryType(lint.ScopeStatic),
}

// RuleScopes returns the rules narrowed to fewer targets than the linter itself runs in.
// It lets `package doc` report where a rule is processed without restating the table.
func RuleScopes() lint.RuleScopes {
	return ruleScopes
}

// Config holds the path, target and settings required to construct a Linter.
type Config struct {
	Definition packages.Definition
	Path       string
	Target     lint.Target
}

// NewLinter constructs a Linter from cfg, scoping its diagnostics to this linter.
func NewLinter(cfg Config, res *diag.Collector) *Linter {
	return &Linter{
		config:    cfg,
		collector: res.With(diag.LinterID(LinterID)),
	}
}

// Linter runs package rules against a package directory.
type Linter struct {
	config Config

	collector *diag.Collector
}

// Lint executes the package rules that apply to the configured scope against the package path.
func (l *Linter) Lint(ctx context.Context) {
	if l.runs(rules.NoWerfRuleID) {
		rules.NewNoWerfRule(l.config.Path, l.collector).Check(ctx)
	}

	if l.runs(rules.NoChartRuleID) {
		rules.NewNoChartRule(l.config.Path, l.collector).Check(ctx)
	}

	if l.runs(rules.NoHelmignoreRuleID) {
		rules.NewNoHelmignoreRule(l.config.Path, l.collector).Check(ctx)
	}

	if l.runs(rules.HasGitignoreRuleID) {
		rules.NewHasGitignoreRule(l.config.Path, l.collector).Check(ctx)
	}

	rules.NewHasChangelogRule(l.config.Path, l.collector).Check(ctx)

	rules.NewHasDocsRule(l.config.Path, l.collector).Check(ctx)

	rules.NewHasIconRule(l.config.Path, l.collector).Check(ctx)

	rules.NewValidRequirementsRule(l.config.Definition.Requirements, l.collector).Check(ctx)
}

// runs reports whether ruleID is processed for the target this linter was built for.
func (l *Linter) runs(ruleID string) bool {
	return ruleScopes.Runs(ruleID, Scopes, l.config.Target)
}
