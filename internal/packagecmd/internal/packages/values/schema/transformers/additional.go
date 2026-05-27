package transformers

import "github.com/go-openapi/spec"

// AdditionalProperties is a Transformer that sets AdditionalProperties to false on
// every object schema node that does not already define it. This prevents values
// documents from containing undeclared keys that would otherwise pass validation silently.
type AdditionalProperties struct{}

// Transform sets AdditionalProperties to false on s when it is unset, then
// recurses into every property, array item, composition branch (allOf /
// anyOf / oneOf), and definition so the same invariant holds for the whole
// tree — regardless of whether intermediate nodes were already constrained.
func (t *AdditionalProperties) Transform(s *spec.Schema) *spec.Schema {
	if s.AdditionalProperties == nil {
		s.AdditionalProperties = &spec.SchemaOrBool{
			Allows: false,
		}
	}

	for k, prop := range s.Properties {
		ts := prop
		s.Properties[k] = *t.Transform(&ts)
	}

	if s.Items != nil {
		if s.Items.Schema != nil {
			s.Items.Schema = t.Transform(s.Items.Schema)
		}

		for i, item := range s.Items.Schemas {
			ts := item
			s.Items.Schemas[i] = *t.Transform(&ts)
		}
	}

	for i, branch := range s.AllOf {
		ts := branch
		s.AllOf[i] = *t.Transform(&ts)
	}

	for i, branch := range s.AnyOf {
		ts := branch
		s.AnyOf[i] = *t.Transform(&ts)
	}

	for i, branch := range s.OneOf {
		ts := branch
		s.OneOf[i] = *t.Transform(&ts)
	}

	for k, def := range s.Definitions {
		ts := def
		s.Definitions[k] = *t.Transform(&ts)
	}

	return s
}
