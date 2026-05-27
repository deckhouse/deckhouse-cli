package rules

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: prevent cyrillic characters from appearing in English documentation files.

// CyrillicInEnglishRuleID is the stable identifier used to reference this rule in configuration.
const CyrillicInEnglishRuleID = "cyrillic-in-english"

var (
	// cyrillicWordPattern matches cyrillic words in English documentation.
	cyrillicWordPattern = regexp.MustCompile(`[А-Яа-яЁё]+`)
	// cyrillicCharPattern marks cyrillic characters in diagnostics.
	cyrillicCharPattern = regexp.MustCompile(`[А-Яа-яЁё]`)
	// nonCyrillicCharPattern replaces non-cyrillic characters in diagnostics.
	nonCyrillicCharPattern = regexp.MustCompile(`[^А-Яа-яЁё]`)
	// russianDocPattern matches canonical Russian documentation files.
	russianDocPattern = regexp.MustCompile(`\.ru\.md$`)
	// russianDocFallbackPattern matches legacy Russian documentation files.
	russianDocFallbackPattern = regexp.MustCompile(`(?i)_ru\.md$`)
)

// CyrillicInEnglishRule enforces that English documentation does not contain cyrillic text.
type CyrillicInEnglishRule struct {
	collector *diag.Collector
	path      string
}

// NewCyrillicInEnglishRule constructs a CyrillicInEnglishRule scoped to path, tagging diagnostics with the rule ID.
func NewCyrillicInEnglishRule(path string, res *diag.Collector) *CyrillicInEnglishRule {
	return &CyrillicInEnglishRule{
		path:      path,
		collector: res.With(diag.RuleID(CyrillicInEnglishRuleID)),
	}
}

// Check scans top-level English markdown files in docs/ for cyrillic characters.
func (r *CyrillicInEnglishRule) Check(_ context.Context) {
	docsPath := filepath.Join(r.path, "docs")

	files, err := collectFiles(docsPath, ".md", ".markdown")
	if err != nil {
		if os.IsNotExist(err) {
			return
		}

		r.collector.With(
			diag.Path("docs"),
			diag.Value(err.Error())).
			Error("cannot read docs directory")

		return
	}

	for _, filePath := range files {
		rel := packageRelativePath(r.path, filePath)
		if filepath.Dir(rel) != "docs" {
			continue
		}

		r.checkFile(filePath)
	}
}

// checkFile reports cyrillic text found in one English documentation file.
func (r *CyrillicInEnglishRule) checkFile(filePath string) {
	relPath := packageRelativePath(r.path, filePath)
	if russianDocPattern.MatchString(filePath) {
		return
	}

	if russianDocFallbackPattern.MatchString(filePath) {
		return
	}

	lines, err := getFileContent(filePath)
	if err != nil {
		r.collector.With(
			diag.Path(relPath),
			diag.Value(err.Error())).
			Error("failed to read file")

		return
	}

	cyrMsg, hasCyr := checkCyrillicLettersInArray(lines)
	if hasCyr {
		r.collector.With(diag.Path(relPath), diag.Value(cyrMsg)).Error("English documentation contains cyrillic characters")
	}
}

// getFileContent reads filename and returns it split into lines.
func getFileContent(filename string) ([]string, error) {
	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return strings.Split(string(fileBytes), "\n"), nil
}

// checkCyrillicLettersInString returns a diagnostic snippet for line when it contains cyrillic text.
func checkCyrillicLettersInString(line string) (string, bool) {
	if !cyrillicWordPattern.MatchString(line) {
		return "", false
	}

	line = strings.TrimSpace(line)

	cursor := nonCyrillicCharPattern.ReplaceAllString(line, "-")
	cursor = cyrillicCharPattern.ReplaceAllString(cursor, "^")
	cursor = strings.TrimRight(cursor, "-")

	return line + "\n" + cursor, true
}

// checkCyrillicLettersInArray returns diagnostic snippets for all lines containing cyrillic text.
func checkCyrillicLettersInArray(lines []string) (string, bool) {
	res := make([]string, 0)

	hasCyr := false

	for i, line := range lines {
		msg, has := checkCyrillicLettersInString(line)
		if has {
			hasCyr = true

			res = append(res, fmt.Sprintf("Line %d: %s", i+1, msg))
		}
	}

	return strings.Join(res, "\n"), hasCyr
}
