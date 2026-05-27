package values

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	// openAPIDir is the subdirectory containing OpenAPI schema files.
	openAPIDir = "openapi"
	// settingsFile is the OpenAPI schema for user-configurable values.
	settingsFile = "settings.yaml"
	// configValuesFile is the legacy name for settingsFile, kept for backward compatibility.
	configValuesFile = "config-values.yaml"
	// valuesFile is the OpenAPI schema for all values including internal ones.
	valuesFile = "values.yaml"
)

// loadSchemas reads settings.yaml (or legacy config-values.yaml) and
// values.yaml from the specified directory. Package schemas:
//
//	/modules/XXX-module-name/openapi/settings.yaml      (preferred)
//	/modules/XXX-module-name/openapi/config-values.yaml (legacy fallback)
//	/modules/XXX-module-name/openapi/values.yaml
func loadSchemas(packageDir string) ([]byte, []byte, error) {
	schemasDir := filepath.Join(packageDir, openAPIDir)

	settingsPath := filepath.Join(schemasDir, settingsFile)

	configValues, err := os.ReadFile(settingsPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("read file '%s': %w", settingsPath, err)
		}

		legacyPath := filepath.Join(schemasDir, configValuesFile)

		configValues, err = os.ReadFile(legacyPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, nil, fmt.Errorf("read file '%s': %w", legacyPath, err)
			}

			configValues = nil
		}
	}

	valuesPath := filepath.Join(schemasDir, valuesFile)

	values, err := os.ReadFile(valuesPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("read file '%s': %w", valuesPath, err)
		}

		values = nil
	}

	return configValues, values, nil
}
