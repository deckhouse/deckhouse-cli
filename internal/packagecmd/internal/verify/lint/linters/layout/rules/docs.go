package rules

import "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"

// Rule purpose: require docs/ so package documentation rules have a directory to inspect.

// DocsRuleID is the stable identifier used to reference this rule in configuration.
const DocsRuleID = "docs"

// NewDocsRule constructs a rule that requires docs/ in the package root.
func NewDocsRule(path string, collector *diag.Collector) *requiredRootPathsRule {
	return newRequiredRootPathsRule(path, collector.With(diag.RuleID(DocsRuleID)), nil, []string{"docs"})
}
