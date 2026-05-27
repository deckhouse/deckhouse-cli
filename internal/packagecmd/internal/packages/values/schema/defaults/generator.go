// Package defaults synthesizes an example value map from an OpenAPI schema.
//
// Generate is the only entry point. It walks the schema's properties and
// produces a value for each one using this precedence:
//
//  1. The `x-example` extension (single illustrative value).
//  2. The `x-examples` extension (first entry, including the OpenAPI 3.0
//     named-examples map form).
//  3. `enum`, preferring `default` if set, else the first enum value.
//  4. Object walk — explicit `type: object`, structural `properties`, or
//     composition keywords (allOf / oneOf / anyOf) contribute properties.
//  5. `default` (deep-copied so callers may mutate the result).
//  6. Array walk — one element synthesized from `items.schema`.
//  7. Scalar placeholder by type: 123 for integer/number, true for boolean,
//     a pattern-matching string via reggen for string.
//
// Properties the generator cannot synthesize (unknown types, dynamic
// additionalProperties, tuple-form arrays) are silently omitted.
//
// Output is deterministic for a given input schema.
package defaults

import (
	"fmt"
	"maps"
	"reflect"

	"github.com/go-openapi/spec"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/values/schema/reggen"
)

// OpenAPI extensions recognized by the generator.
const (
	xExample  = "x-example"
	xExamples = "x-examples"
)

// JSON Schema type names checked via spec.StringOrArray.Contains.
const (
	typeObject  = "object"
	typeArray   = "array"
	typeString  = "string"
	typeInteger = "integer"
	typeNumber  = "number"
	typeBoolean = "boolean"
)

// Placeholder scalars returned for type-only schema fields with no default
// or example to draw from. Integer and number are kept distinct so the
// JSON type of the result matches the schema's declared type.
const (
	placeholderInteger         = 123
	placeholderNumber  float64 = 123.0
	placeholderBoolean         = true
)

// String-synthesis defaults. The pattern is used when a string property has
// no `pattern` set; the repeat limit caps how many characters reggen produces
// for unbounded patterns like `[a-z]+`.
const (
	defaultStringPattern = `^[a-zA-Z0-9]{8}$`
	stringRepeatLimit    = 8
)

// Generate synthesizes an example value map from an OpenAPI schema.
//
// Returns (nil, nil) when root is nil, which is the contract callers rely
// on for optional schema files that may be absent on disk.
//
// The returned map and all nested values are freshly allocated, so callers
// may mutate the result without affecting the schema in memory.
func Generate(root *spec.Schema) (map[string]any, error) {
	if root == nil {
		return nil, nil
	}

	return synthesizeObject(root)
}

// synthesizeObject builds an example map from the effective property set
// of s — its own Properties plus those contributed by one level of allOf /
// oneOf / anyOf branches. An empty (non-nil) map is returned when there
// are no properties, so the recursion can distinguish "object with no
// fields" from "no synthesis possible."
func synthesizeObject(s *spec.Schema) (map[string]any, error) {
	props := mergedProperties(s)

	out := make(map[string]any, len(props))
	for name, prop := range props {
		v, err := synthesizeValue(&prop)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", name, err)
		}

		if v != nil {
			out[name] = v
		}
	}

	return out, nil
}

// synthesizeValue produces an example value for a single schema node.
// See the package doc for precedence rules. Returns (nil, nil) when none
// of the rules apply, signalling "leave the property unset in the parent."
func synthesizeValue(s *spec.Schema) (any, error) {
	if s == nil {
		return nil, nil
	}

	if ex, ok := s.Extensions[xExample]; ok && ex != nil {
		return overlayExample(s, ex)
	}

	if ex, ok := s.Extensions[xExamples]; ok {
		if first, ok := firstExample(ex); ok {
			return overlayExample(s, first)
		}
	}

	if len(s.Enum) > 0 {
		if s.Default != nil {
			for _, e := range s.Enum {
				if reflect.DeepEqual(e, s.Default) {
					return deepCopyJSON(s.Default), nil
				}
			}
		}

		return deepCopyJSON(s.Enum[0]), nil
	}

	if isObject(s) {
		return synthesizeObject(s)
	}

	if s.Default != nil {
		return deepCopyJSON(s.Default), nil
	}

	if s.Type.Contains(typeArray) && s.Items != nil && s.Items.Schema != nil {
		elem, err := synthesizeValue(s.Items.Schema)
		if err != nil {
			return nil, err
		}

		return []any{elem}, nil
	}

	switch {
	case s.Type.Contains(typeString):
		return synthesizeString(s.Pattern)
	case s.Type.Contains(typeInteger):
		return placeholderInteger, nil
	case s.Type.Contains(typeNumber):
		return placeholderNumber, nil
	case s.Type.Contains(typeBoolean):
		return placeholderBoolean, nil
	}

	return nil, nil
}

// isObject reports whether s should be walked as an object. We accept three
// signals: an explicit `type: object`, a structural Properties block, or
// the presence of any composition keyword whose branches typically declare
// properties. The structural cue catches real-world schemas that omit
// `type:` on nested object definitions.
func isObject(s *spec.Schema) bool {
	if s.Type.Contains(typeObject) || len(s.Properties) > 0 {
		return true
	}

	return len(s.AllOf) > 0 || len(s.OneOf) > 0 || len(s.AnyOf) > 0
}

// mergedProperties returns s.Properties merged with one level of composed
// branches' Properties. s.Properties takes precedence over any contribution
// from allOf / oneOf / anyOf, so a schema can override an inherited field.
//
// Combiner semantics:
//   - allOf: every branch must match, so all branches' Properties are merged.
//   - oneOf: exactly one branch matches, so only the first branch contributes
//     (deterministic pick — merging would produce an object valid against none).
//   - anyOf: at least one branch matches, so the first branch contributes
//     a minimal example.
//
// Composition is flattened only one level deep. Branches that themselves
// declare composition keywords contribute their direct properties; their
// own allOf/oneOf/anyOf are not unrolled. This is sufficient for the schemas
// the runtime encounters and avoids the complexity of full schema-composition
// semantics.
func mergedProperties(s *spec.Schema) map[string]spec.Schema {
	props := make(map[string]spec.Schema)
	for _, branch := range s.AllOf {
		maps.Copy(props, branch.Properties)
	}

	if len(s.OneOf) > 0 {
		maps.Copy(props, s.OneOf[0].Properties)
	}

	if len(s.AnyOf) > 0 {
		maps.Copy(props, s.AnyOf[0].Properties)
	}

	maps.Copy(props, s.Properties)

	return props
}

// overlayExample renders an example value. When the schema is an object
// and the example is a map, the synthesized object is built first and then
// the example is layered on top — this lets schema authors write a partial
// `x-example` and have unset fields filled from per-property defaults.
//
// For non-object schemas, the example is returned as a deep clone.
func overlayExample(s *spec.Schema, example any) (any, error) {
	exMap, ok := example.(map[string]any)
	if !ok || !isObject(s) {
		return deepCopyJSON(example), nil
	}

	base, err := synthesizeObject(s)
	if err != nil {
		return nil, err
	}

	deepMergeOverride(base, exMap)

	return base, nil
}

// firstExample picks a representative value out of an x-examples block.
// Three forms are recognized:
//
//   - []any            — first element wins.
//   - []map[string]any — first element wins; less common parse output.
//   - map[string]any   — OpenAPI 3.0 named-examples form. Go map iteration
//     order is unspecified, but this output is for example data so
//     reproducibility is best-effort.
//
// Returns (nil, false) when no form matches or the container is empty.
func firstExample(v any) (any, bool) {
	switch xs := v.(type) {
	case []any:
		if len(xs) > 0 {
			return xs[0], true
		}
	case []map[string]any:
		if len(xs) > 0 {
			return xs[0], true
		}
	case map[string]any:
		for _, val := range xs {
			return val, true
		}
	}

	return nil, false
}

// synthesizeString delegates to reggen to produce a value matching the
// schema's `pattern`. An empty pattern is replaced with an alphanumeric
// 8-character default so type-only string fields still get a placeholder.
func synthesizeString(pattern string) (string, error) {
	if pattern == "" {
		pattern = defaultStringPattern
	}

	s, err := reggen.Generate(pattern, stringRepeatLimit)
	if err != nil {
		return "", fmt.Errorf("synthesize string for pattern %q: %w", pattern, err)
	}

	return s, nil
}

// deepMergeOverride copies entries from src into dst, recursing into map
// values so nested fields merge instead of replacing whole subtrees. On
// any conflict the src value wins. Non-map src values are deep-cloned
// before insertion so subsequent mutations of dst cannot reach back into
// the original example or schema.
func deepMergeOverride(dst, src map[string]any) {
	for k, sv := range src {
		if dm, ok := dst[k].(map[string]any); ok {
			if sm, ok := sv.(map[string]any); ok {
				deepMergeOverride(dm, sm)
				continue
			}
		}

		dst[k] = deepCopyJSON(sv)
	}
}

// deepCopyJSON returns a deep clone of a JSON-decodable value. Used when
// inserting schema-owned data (defaults, examples) into the result so that
// caller mutations cannot reach back into the schema.
//
// Panics on values outside the JSON value set. This surfaces schema-author
// errors (for instance a Go time.Time slipped into an extension) at synthesis
// time rather than corrupting downstream output.
func deepCopyJSON(v any) any {
	switch x := v.(type) {
	case map[string]any:
		if x == nil {
			return x
		}

		out := make(map[string]any, len(x))
		for k, w := range x {
			out[k] = deepCopyJSON(w)
		}

		return out
	case []any:
		if x == nil {
			return x
		}

		out := make([]any, len(x))
		for i, w := range x {
			out[i] = deepCopyJSON(w)
		}

		return out
	case string, bool, float64, int, int64, nil:
		return x
	default:
		panic(fmt.Errorf("defaults: cannot deep copy %T", v))
	}
}
