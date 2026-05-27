package rules

import "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"

// Rule purpose: require .gitignore so local and generated files stay out of package repositories.

// GitignoreRuleID is the stable identifier used to reference this rule in configuration.
const GitignoreRuleID = "gitignore"

// NewGitignoreRule constructs a rule that requires .gitignore in the package root.
func NewGitignoreRule(path string, collector *diag.Collector) *requiredRootPathsRule {
	return newRequiredRootPathsRule(path, collector.With(diag.RuleID(GitignoreRuleID)), []string{".gitignore"}, nil)
}
