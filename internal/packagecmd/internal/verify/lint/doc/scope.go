package doc

import (
	"fmt"
	"slices"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss"
	pkglint "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/package"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates"
)

// linterScopes maps a linter ID to the targets it is processed in, taken from the
// linter packages themselves. Membership is a code-level decision — .pkglint.yaml
// tunes severities and can silence a rule but cannot switch one on — so the
// documented targets are read from the same declarations verify enforces.
var linterScopes = map[string]lint.TypeScopes{
	pkglint.LinterID:   pkglint.Scopes,
	templates.LinterID: templates.Scopes,
	docs.LinterID:      docs.Scopes,
	images.LinterID:    images.Scopes,
	icon.LinterID:      icon.Scopes,
	oss.LinterID:       oss.Scopes,
}

// ruleScopeTables holds the per-rule narrowings of the linters that declare any. A
// linter absent from the map narrows nothing, so each of its rules is processed
// wherever the linter is.
var ruleScopeTables = map[string]lint.RuleScopes{
	pkglint.LinterID:   pkglint.RuleScopes(),
	templates.LinterID: templates.RuleScopes(),
}

// configPaths maps a scope to the .pkglint.yaml key holding its copy of the linter tree.
var configPaths = map[lint.Scope]string{
	lint.ScopeStatic:  string(lint.ScopeStatic),
	lint.ScopeBundle:  "remote." + string(lint.ScopeBundle),
	lint.ScopeRelease: "remote." + string(lint.ScopeRelease),
}

// scopesOf returns the targets a linter is processed in.
func scopesOf(linterID string) lint.TypeScopes {
	return linterScopes[linterID]
}

// ruleScopesOf returns the targets a rule is processed in, which are its linter's
// own unless the rule is narrowed to fewer.
func ruleScopesOf(linterID, ruleID string) lint.TypeScopes {
	if scopes, ok := ruleScopeTables[linterID][ruleID]; ok {
		return scopes
	}

	return scopesOf(linterID)
}

// scopeLines describes the targets as one or more display lines. Package types that
// share the same scopes collapse into a single line; otherwise each type gets its
// own, so a rule limited to one package type cannot read as universal.
func scopeLines(scopes lint.TypeScopes) []string {
	if shared, ok := sharedScopes(scopes); ok {
		return []string{"runs in: " + strings.Join(scopeNames(shared), ", ")}
	}

	lines := make([]string, 0, len(scopes))

	for _, packageType := range lint.AllTypes {
		if len(scopes[packageType]) == 0 {
			continue
		}

		lines = append(lines, fmt.Sprintf("runs in: %s (%s packages only)",
			strings.Join(scopeNames(scopes[packageType]), ", "), packageType))
	}

	return lines
}

// sharedScopes returns the scopes every package type is processed in, reporting
// false when the types do not all carry the same set.
func sharedScopes(scopes lint.TypeScopes) (lint.Scopes, bool) {
	var shared lint.Scopes

	for i, packageType := range lint.AllTypes {
		typeScopes, ok := scopes[packageType]
		if !ok {
			return nil, false
		}

		if i == 0 {
			shared = typeScopes

			continue
		}

		if !slices.Equal(shared, typeScopes) {
			return nil, false
		}
	}

	return shared, len(shared) > 0
}

// scopeNames renders scopes as their names, in canonical order.
func scopeNames(scopes lint.Scopes) []string {
	names := make([]string, 0, len(scopes))

	for _, scope := range lint.AllScopes {
		if scopes.Contains(scope) {
			names = append(names, string(scope))
		}
	}

	return names
}

// configPathsOf renders the .pkglint.yaml keys that configure the targets, covering
// every scope at least one package type is processed in.
func configPathsOf(scopes lint.TypeScopes) []string {
	paths := make([]string, 0, len(lint.AllScopes))

	for _, scope := range lint.AllScopes {
		if processedIn(scopes, scope) {
			paths = append(paths, configPaths[scope])
		}
	}

	return paths
}

// processedIn reports whether any package type is processed in scope.
func processedIn(scopes lint.TypeScopes, scope lint.Scope) bool {
	for _, typeScopes := range scopes {
		if typeScopes.Contains(scope) {
			return true
		}
	}

	return false
}
