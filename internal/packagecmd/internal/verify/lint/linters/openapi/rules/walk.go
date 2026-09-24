package rules

import (
	"strconv"
	"strings"
)

// Every rule that has something to say about a schema has to find one wherever it sits in
// a document, so the descent into nested schemas lives here rather than in a rule.

// Nested-schema keywords, grouped by the shape of the value they hold and mapped to the
// property levels descending into them adds. Traversal follows these rather than every map
// in the document, so a setting named like a keyword is never mistaken for one.
//
// Iteration order does not matter: every rule sorts its findings before reporting them.
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

// walkSchemas calls visit for root and for every schema nested below it, passing the
// pointer that locates the schema in the document and the property levels between it and
// the root. Depth counts settings rather than YAML nesting, so a combinator branch stays
// at the level of the schema it constrains.
func walkSchemas(root map[string]any, visit func(node map[string]any, pointer string, depth int)) {
	walkSchema(root, "", 0, visit)
}

// walkSchema visits node and descends into the nested schemas below it.
func walkSchema(node map[string]any, pointer string, depth int, visit func(map[string]any, string, int)) {
	visit(node, pointer, depth)

	for keyword, delta := range namedSchemaKeywords {
		named, ok := node[keyword].(map[string]any)
		if !ok {
			continue
		}

		for name, value := range named {
			if child, ok := value.(map[string]any); ok {
				walkSchema(child, joinPointer(pointer, keyword, name), depth+delta, visit)
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
				walkSchema(child, joinPointer(pointer, keyword, strconv.Itoa(i)), depth+delta, visit)
			}
		}
	}

	for keyword, delta := range singleSchemaKeywords {
		if child, ok := node[keyword].(map[string]any); ok {
			walkSchema(child, joinPointer(pointer, keyword), depth+delta, visit)
		}
	}
}

// joinPointer appends segments to a parent pointer, dot-separated, skipping the empty
// ones. Keywords stay in the result so the pointer maps straight onto the lines of the
// schema file.
func joinPointer(pointer string, segments ...string) string {
	parts := make([]string, 0, len(segments)+1)

	for _, segment := range append([]string{pointer}, segments...) {
		if segment != "" {
			parts = append(parts, segment)
		}
	}

	return strings.Join(parts, ".")
}
