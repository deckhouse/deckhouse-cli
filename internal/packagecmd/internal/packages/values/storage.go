package values

import (
	"fmt"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/values/schema"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/values/schema/defaults"
)

// Storage holds example values synthesized from a package's OpenAPI
// settings and values schemas. Both maps are produced once at construction
// by defaults.Generate; the accessors are pure getters that return the
// cached instances.
//
// Either map may be nil if the corresponding schema file was missing.
// The returned maps are shared references — callers must not mutate them.
type Storage struct {
	settings     map[string]any
	resultValues map[string]any
}

// LoadStorage loads the OpenAPI schemas from path/openapi (settings.yaml or
// the legacy config-values.yaml, plus values.yaml), compiles them, and
// synthesizes an example map from each. The synthesized maps are served
// unchanged by GetSettings and GetValues.
//
// Missing schema files are tolerated: the corresponding example map will
// be nil.
//
// Returns an error if a schema file is unreadable, schema compilation
// fails, or example synthesis fails.
func LoadStorage(path string) (*Storage, error) {
	settingsBytes, valuesBytes, err := loadSchemas(path)
	if err != nil {
		return nil, fmt.Errorf("load schemas: %w", err)
	}

	schemaStorage, err := schema.NewStorage(settingsBytes, valuesBytes)
	if err != nil {
		return nil, fmt.Errorf("new schema storage: %w", err)
	}

	settings, err := defaults.Generate(schemaStorage.GetSchema(schema.TypeSettings))
	if err != nil {
		return nil, fmt.Errorf("generate settings: %w", err)
	}

	resultValues, err := defaults.Generate(schemaStorage.GetSchema(schema.TypeValues))
	if err != nil {
		return nil, fmt.Errorf("generate result values: %w", err)
	}

	s := &Storage{
		settings:     settings,
		resultValues: resultValues,
	}

	return s, nil
}

// GetValues returns the example values synthesized from the values schema,
// or nil if no values schema was present. The returned map is the cached
// instance — do not mutate.
func (s *Storage) GetValues() map[string]any {
	return s.resultValues
}

// GetSettings returns the example settings synthesized from the settings
// schema, or nil if no settings schema was present. The returned map is
// the cached instance — do not mutate.
func (s *Storage) GetSettings() map[string]any {
	return s.settings
}
