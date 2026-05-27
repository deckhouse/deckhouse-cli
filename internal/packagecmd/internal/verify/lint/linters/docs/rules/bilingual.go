package rules

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require Russian translations for each top-level English markdown file in docs/.

// BilingualRuleID is the stable identifier used to reference this rule in configuration.
const BilingualRuleID = "bilingual"

// Russian documentation suffixes accepted by the bilingual rule.
const (
	ruSuffix         = ".ru.md"
	ruFallbackSuffix = "_ru.md"
)

// BilingualRule enforces that top-level English docs have Russian counterparts.
type BilingualRule struct {
	collector *diag.Collector
	path      string
}

// NewBilingualRule constructs a BilingualRule scoped to path, tagging diagnostics with the rule ID.
func NewBilingualRule(path string, res *diag.Collector) *BilingualRule {
	return &BilingualRule{
		path:      path,
		collector: res.With(diag.RuleID(BilingualRuleID)),
	}
}

// Check verifies that each top-level English markdown file in docs/ has a Russian translation.
func (r *BilingualRule) Check(_ context.Context) {
	docsPath := filepath.Join(r.path, "docs")
	if _, err := os.Stat(docsPath); err != nil {
		return
	}

	files, err := collectFiles(docsPath, ".md")
	if err != nil {
		r.collector.With(
			diag.Path("docs"),
			diag.Value(err.Error())).
			Error("cannot read docs directory")

		return
	}

	fileSet := make(map[string]struct{}, len(files))
	for _, filePath := range files {
		rel := packageRelativePath(r.path, filePath)
		if filepath.Dir(rel) != "docs" {
			continue
		}

		if strings.HasSuffix(strings.ToLower(rel), ruFallbackSuffix) {
			rel = strings.ToLower(rel)
		}

		fileSet[rel] = struct{}{}
	}

	for rel := range fileSet {
		if !strings.HasPrefix(rel, "docs/") {
			continue
		}

		if !strings.HasSuffix(rel, ".md") || strings.HasSuffix(rel, ruFallbackSuffix) || strings.HasSuffix(rel, ruSuffix) {
			continue
		}

		base := strings.TrimSuffix(rel, ".md")

		// TODO: Delete it after renaming to .ru.md view.
		ruRelFallback := strings.ToLower(base) + ruFallbackSuffix
		if _, ok := fileSet[ruRelFallback]; ok {
			continue
		}

		ruRel := base + ruSuffix
		if _, ok := fileSet[ruRel]; !ok {
			r.collector.With(diag.Path(rel)).Error("Russian counterpart is missing: need to create a matching .ru.md in docs/")
		}
	}
}
