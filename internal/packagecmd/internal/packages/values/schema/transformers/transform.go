package transformers

import "github.com/go-openapi/spec"

// Transformer mutates or replaces an OpenAPI spec.Schema, returning the result.
type Transformer interface {
	Transform(s *spec.Schema) *spec.Schema
}

// Transform applies each Transformer in order to s, returning the final schema.
// Returns nil if s is nil.
func Transform(s *spec.Schema, transformers ...Transformer) *spec.Schema {
	if s == nil {
		return nil
	}

	for _, transformer := range transformers {
		s = transformer.Transform(s)
	}

	return s
}
