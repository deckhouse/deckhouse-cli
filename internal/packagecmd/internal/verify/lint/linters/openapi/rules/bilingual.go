package rules

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require a Russian translation next to every schema that documents
// user-facing settings.

// BilingualRuleID is the stable identifier used to reference this rule in configuration.
const BilingualRuleID = "bilingual"

// BilingualRule checks that the schemas under openapi/ and their translations come in pairs.
type BilingualRule struct {
	collector *diag.Collector
	path      string
}

// NewBilingualRule constructs a BilingualRule scoped to a package directory. Which schemas
// it pairs up is fixed, so the rule resolves them from packageDir itself.
func NewBilingualRule(packageDir string, collector *diag.Collector) *BilingualRule {
	return &BilingualRule{
		path:      packageDir,
		collector: collector.With(diag.RuleID(BilingualRuleID)),
	}
}

// Check reports a schema whose translation is missing and a translation whose schema is
// missing, the latter being how a misspelled translation surfaces. values.yaml is exempt:
// it describes the values computed for the templates rather than the settings a user
// writes, so it carries no user-facing descriptions to translate. So are the test
// fixtures, which hold values rather than a schema.
func (r *BilingualRule) Check(_ context.Context) {
	names, err := schemaNames(r.path)
	if err != nil {
		r.collector.With(
			diag.Path(openAPIDir),
			diag.Value(err.Error())).
			Error("cannot read the openapi directory")

		return
	}

	present := make(map[string]struct{}, len(names))
	for _, name := range names {
		present[name] = struct{}{}
	}

	for _, name := range names {
		if isTestFixture(name) {
			continue
		}

		collector := r.collector.With(diag.Path(filepath.Join(openAPIDir, name)))

		if isTranslation(name) {
			translated := strings.TrimPrefix(name, docRuPrefix)
			if _, ok := present[translated]; !ok {
				collector.Error("translation has nothing to translate: %s is missing next to it", translated)
			}

			continue
		}

		if name == valuesFile {
			continue
		}

		if _, ok := present[docRuPrefix+name]; !ok {
			collector.Error("translation is missing: need to create %s next to it", docRuPrefix+name)
		}
	}

	r.collector.Commit()
}
