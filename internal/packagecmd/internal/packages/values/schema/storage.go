package schema

import (
	"encoding/json"
	"fmt"

	"github.com/go-openapi/spec"
	"github.com/go-openapi/swag/yamlutils"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/values/schema/transformers"
)

// Type identifies which schema variant is used for a given validation context.
type Type string

const (
	// TypeSettings is the schema for user-supplied configuration values (config.yaml).
	TypeSettings Type = "config"
	// TypeValues is the schema for the full set of internal module values.
	TypeValues Type = "values"
)

// Storage holds compiled OpenAPI schemas for a package, keyed by Type.
// All schemas are pre-processed and ready for repeated validation calls.
type Storage struct {
	schemas map[Type]*spec.Schema
}

// NewStorage parses settings and values YAML schema documents, applies the
// required transformations, and returns a Storage ready for use.
func NewStorage(settings, values []byte) (*Storage, error) {
	schemas, err := prepareSchemas(settings, values)
	if err != nil {
		return nil, fmt.Errorf("prepare schemas: %w", err)
	}

	return &Storage{schemas: schemas}, err
}

// GetSchema returns schema by Type
func (s *Storage) GetSchema(schemaType Type) *spec.Schema {
	return s.schemas[schemaType]
}

// prepareSchemas parses the settings and values YAML documents, applies the
// transformer pipeline to each, and returns a map keyed by Type. The values
// schema receives an additional Extend transformer so it may inherit fields
// from the settings schema via the x-extend extension.
func prepareSchemas(settings, values []byte) (map[Type]*spec.Schema, error) {
	res := make(map[Type]*spec.Schema)

	if len(settings) > 0 {
		schemaObj, err := loadSchemaFromBytes(settings)
		if err != nil {
			return nil, fmt.Errorf("load '%s' schema: %w", TypeSettings, err)
		}

		res[TypeSettings] = transformers.Transform(
			schemaObj,
			&transformers.AdditionalProperties{},
		)
	}

	if len(values) > 0 {
		schemaObj, err := loadSchemaFromBytes(values)
		if err != nil {
			return nil, fmt.Errorf("load '%s' schema: %w", TypeValues, err)
		}

		res[TypeValues] = transformers.Transform(
			schemaObj,
			&transformers.Extend{Parent: res[TypeSettings]},
			&transformers.AdditionalProperties{},
		)
	}

	return res, nil
}

// loadSchemaFromBytes returns spec.Schema object loaded from YAML bytes.
func loadSchemaFromBytes(openAPIContent []byte) (*spec.Schema, error) {
	jsonDoc, err := yamlBytesToJSONDoc(openAPIContent)
	if err != nil {
		return nil, err
	}

	s := new(spec.Schema)
	if err = json.Unmarshal(jsonDoc, s); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	if err = spec.ExpandSchema(s, s, nil); err != nil {
		return nil, fmt.Errorf("expand schema: %w", err)
	}

	return s, nil
}

// yamlBytesToJSONDoc is a replacement of swag.YAMLData and YAMLDoc to Unmarshal into interface{}.
// swag.BytesToYAML uses yaml.MapSlice to unmarshal YAML. This type doesn't support map merge of YAML anchors.
func yamlBytesToJSONDoc(data []byte) (json.RawMessage, error) {
	var yamlObj any
	if err := yaml.Unmarshal(data, &yamlObj); err != nil {
		return nil, fmt.Errorf("yaml unmarshal: %w", err)
	}

	doc, err := yamlutils.YAMLToJSON(yamlObj)
	if err != nil {
		return nil, fmt.Errorf("yaml to json: %w", err)
	}

	return doc, nil
}
