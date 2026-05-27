package rules

import "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"

// Rule purpose: require changelog.yaml so package changes have a release history entry point.

// ChangelogRuleID is the stable identifier used to reference this rule in configuration.
const ChangelogRuleID = "changelog"

// NewChangelogRule constructs a rule that requires changelog.yaml in the package root.
func NewChangelogRule(path string, collector *diag.Collector) *requiredRootPathsRule {
	return newRequiredRootPathsRule(path, collector.With(diag.RuleID(ChangelogRuleID)), []string{"changelog.yaml"}, nil)
}
