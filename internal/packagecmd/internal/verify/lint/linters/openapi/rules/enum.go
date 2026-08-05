package rules

import (
	"cmp"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"unicode"

	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: keep the enum values of the schemas in the CamelCase the Kubernetes API
// conventions prescribe for constants.

// EnumRuleID is the stable identifier used to reference this rule in configuration.
const EnumRuleID = "enum"

// enumKeyword is the schema keyword listing the values a setting is allowed to take.
const enumKeyword = "enum"

// EnumRule checks that every enum value in a package's schemas is CamelCase.
type EnumRule struct {
	collector *diag.Collector
	path      string
}

// NewEnumRule constructs an EnumRule scoped to a package directory. Which schemas it
// reads is fixed, so the rule resolves them from packageDir itself.
func NewEnumRule(packageDir string, collector *diag.Collector) *EnumRule {
	return &EnumRule{
		path:      packageDir,
		collector: collector.With(diag.RuleID(EnumRuleID)),
	}
}

// Check reports every enum value of every schema that breaks the CamelCase convention.
// Translations are skipped because they carry descriptions rather than values, and so are
// test fixtures, which hold values rather than a schema.
//
// Findings are warnings. The convention is what Kubernetes prescribes for its own API
// constants, but a package whose settings pass values straight through to an upstream
// configuration file cannot follow it without translating every value back in the
// template, so the rule advises rather than blocks.
func (r *EnumRule) Check(_ context.Context) {
	names, err := schemaNames(r.path)
	if err != nil {
		r.collector.With(
			diag.Path(openAPIDir),
			diag.Value(err.Error())).
			Warn("cannot read the openapi directory")

		return
	}

	for _, name := range names {
		if isTranslation(name) || isTestFixture(name) {
			continue
		}

		r.checkSchema(name)
	}

	r.collector.Commit()
}

// checkSchema reports the invalid enum values of one schema file.
func (r *EnumRule) checkSchema(name string) {
	collector := r.collector.With(diag.Path(filepath.Join(openAPIDir, name)))

	raw, err := os.ReadFile(filepath.Join(r.path, openAPIDir, name))
	if err != nil {
		collector.Warn("failed to read %s: %v", name, err)

		return
	}

	var root map[string]any
	if err = yaml.Unmarshal(raw, &root); err != nil {
		collector.Warn("failed to parse %s: %v", name, err)

		return
	}

	for _, finding := range invalidEnumValues(root) {
		collector.With(diag.Value(finding.pointer)).
			Warn("enum value %q %s", finding.value, finding.reason)
	}
}

// enumFinding is one invalid value of one enum.
type enumFinding struct {
	// pointer locates the enum keyword holding the value inside the schema.
	pointer string
	// value is the offending enum value.
	value string
	// reason states what the value breaks.
	reason string
}

// invalidEnumValues returns a finding for every enum value in root that breaks the
// convention, sorted so the same schema always reports in the same order.
func invalidEnumValues(root map[string]any) []enumFinding {
	var found []enumFinding

	collectInvalidEnums(root, "", &found)

	slices.SortFunc(found, func(a, b enumFinding) int {
		return cmp.Or(
			cmp.Compare(a.pointer, b.pointer),
			cmp.Compare(a.value, b.value))
	})

	return found
}

// collectInvalidEnums walks everything below node, appending a finding for each invalid
// value of every enum it passes. The whole document is walked rather than the
// nested-schema keywords alone, because an enum constrains a value wherever it appears;
// a setting that merely shares the keyword's name holds a schema instead of a list of
// values, which is what the type check keeps apart.
func collectInvalidEnums(node map[string]any, pointer string, found *[]enumFinding) {
	for key, value := range node {
		keyPointer := joinPointer(pointer, key)

		if values, ok := value.([]any); ok && key == enumKeyword {
			*found = append(*found, invalidValues(keyPointer, values)...)

			continue
		}

		switch nested := value.(type) {
		case map[string]any:
			collectInvalidEnums(nested, keyPointer, found)
		case []any:
			for i, item := range nested {
				if child, ok := item.(map[string]any); ok {
					collectInvalidEnums(child, joinPointer(keyPointer, strconv.Itoa(i)), found)
				}
			}
		}
	}
}

// invalidValues returns a finding for each value of one enum that breaks the convention.
// A value that is not a string is skipped: a boolean or numeric constant carries no casing.
func invalidValues(pointer string, values []any) []enumFinding {
	findings := make([]enumFinding, 0, len(values))

	for _, value := range values {
		str, ok := value.(string)
		if !ok {
			continue
		}

		if reason := conventionBreach(str); reason != "" {
			findings = append(findings, enumFinding{
				pointer: pointer,
				value:   str,
				reason:  reason,
			})
		}
	}

	return findings
}

// conventionBreach returns what value breaks in the CamelCase convention, or an empty
// string when it breaks nothing. An empty value stands for "unset" rather than a name,
// so it is accepted, and a value opening with a non-letter is left to start as it does:
// the leading-capital requirement is about words, not about digits or symbols.
func conventionBreach(value string) string {
	if value == "" {
		return ""
	}

	runes := []rune(value)

	if unicode.IsLetter(runes[0]) && !unicode.IsUpper(runes[0]) {
		return "must start with a capital letter"
	}

	for i, char := range runes {
		switch {
		case unicode.IsLetter(char), unicode.IsNumber(char):
			continue
		// A dot belongs to a version or a fraction, so it has to follow a digit.
		case char == '.' && i > 0 && unicode.IsNumber(runes[i-1]):
			continue
		default:
			return "must be in CamelCase"
		}
	}

	return ""
}
