package rules

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: keep the UI-advanced marker at the top level of the settings schema, the
// only depth the UI reads it from.

// AdvancedRuleID is the stable identifier used to reference this rule in diagnostics.
const AdvancedRuleID = "advanced"

const (
	// openAPIDir is the package subdirectory holding the OpenAPI schemas.
	openAPIDir = "openapi"
	// settingsFile is the OpenAPI schema describing user-configurable settings.
	settingsFile = "settings.yaml"
	// advancedKey is the vendor extension that marks a setting as advanced in the UI.
	advancedKey = "x-deckhouse-ui-advanced"
	// maxAdvancedDepth is the deepest property level the marker is honoured at. Depth 0 is
	// the schema root and depth 1 its top-level settings; below that the UI ignores it.
	maxAdvancedDepth = 1
)

// Nested-schema keywords, grouped by the shape of the value they hold and mapped to the
// property levels descending into them adds. Traversal follows these rather than every map
// in the document, so a setting named like the extension is never mistaken for it.
//
// Iteration order does not matter: findings are sorted before they are reported.
var (
	// namedSchemaKeywords hold a map of name to nested schema.
	namedSchemaKeywords = map[string]int{
		"properties":        1,
		"patternProperties": 1,
		// A definition stands in for a setting schema, so its body sits at the level a
		// top-level setting does. Where it is referenced from is not knowable here.
		"definitions": 1,
		"$defs":       1,
	}

	// listSchemaKeywords hold an ordered list of nested schemas. A combinator branch
	// constrains the same object as its parent, so it stays at the parent's level; the
	// tuple form of items describes elements, which is one level down.
	listSchemaKeywords = map[string]int{
		"allOf": 0,
		"anyOf": 0,
		"oneOf": 0,
		"items": 1,
	}

	// singleSchemaKeywords hold exactly one nested schema. items appears here and in
	// listSchemaKeywords because it takes both forms; each loop type-checks its value, so
	// a given document shape is visited by exactly one of them.
	singleSchemaKeywords = map[string]int{
		"items":                1,
		"additionalProperties": 1,
		"not":                  0,
	}
)

// AdvancedRule checks that the settings schema carries the advanced marker at its top
// level only.
type AdvancedRule struct {
	collector *diag.Collector
	path      string
}

// NewAdvancedRule constructs an AdvancedRule scoped to a package directory. The
// schema location inside it is fixed, so the rule resolves it from packageDir itself.
func NewAdvancedRule(packageDir string, collector *diag.Collector) *AdvancedRule {
	return &AdvancedRule{
		path: packageDir,
		collector: collector.With(
			diag.RuleID(AdvancedRuleID),
			diag.Path(filepath.Join(openAPIDir, settingsFile))),
	}
}

// Check reports every schema below the top level that carries the advanced marker. The
// marker is optional: a schema that never sets it, and one that sets it on the root object
// or on its top-level settings, all satisfy the rule. An absent settings.yaml means the
// package exposes no settings, which is not this rule's concern.
func (r *AdvancedRule) Check(_ context.Context) {
	raw, err := os.ReadFile(filepath.Join(r.path, openAPIDir, settingsFile))
	if os.IsNotExist(err) {
		return
	}

	if err != nil {
		r.collector.Error("failed to read %s: %v", settingsFile, err)

		return
	}

	var root map[string]any
	if err = yaml.Unmarshal(raw, &root); err != nil {
		r.collector.Error("failed to parse %s: %v", settingsFile, err)

		return
	}

	for _, pointer := range deepAdvanced(root) {
		r.collector.With(diag.Value(pointer)).
			Error("%s is allowed only on root properties", advancedKey)
	}

	r.collector.Commit()
}

// deepAdvanced returns the pointer of every schema below the top level of root that
// carries the advanced marker, sorted so the same schema always reports in the same order.
func deepAdvanced(root map[string]any) []string {
	var found []string

	collectAdvanced(root, "", 0, &found)

	slices.Sort(found)

	return found
}

// collectAdvanced appends the pointer of each schema at or below node that carries the
// advanced marker deeper than the UI honours it, descending through the nested-schema
// keywords. depth counts the property levels between node and the schema root.
func collectAdvanced(node map[string]any, pointer string, depth int, found *[]string) {
	if depth > maxAdvancedDepth {
		if _, ok := node[advancedKey]; ok {
			*found = append(*found, pointer)
		}
	}

	for keyword, delta := range namedSchemaKeywords {
		named, ok := node[keyword].(map[string]any)
		if !ok {
			continue
		}

		for name, value := range named {
			if child, ok := value.(map[string]any); ok {
				collectAdvanced(child, joinPointer(pointer, keyword, name), depth+delta, found)
			}
		}
	}

	for keyword, delta := range listSchemaKeywords {
		list, ok := node[keyword].([]any)
		if !ok {
			continue
		}

		for i, item := range list {
			if child, ok := item.(map[string]any); ok {
				collectAdvanced(child, joinPointer(pointer, keyword, strconv.Itoa(i)), depth+delta, found)
			}
		}
	}

	for keyword, delta := range singleSchemaKeywords {
		if child, ok := node[keyword].(map[string]any); ok {
			collectAdvanced(child, joinPointer(pointer, keyword), depth+delta, found)
		}
	}
}

// joinPointer appends segments to a parent pointer, dot-separated. Keywords stay in the
// result so the pointer maps straight onto the lines of settings.yaml.
func joinPointer(pointer string, segments ...string) string {
	if pointer != "" {
		segments = append([]string{pointer}, segments...)
	}

	return strings.Join(segments, ".")
}
