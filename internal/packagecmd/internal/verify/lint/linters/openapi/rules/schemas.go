package rules

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Every rule in this package reads the schemas a package ships under openapi/, so the
// listing and the filename conventions that classify one live here rather than in a rule.

const (
	// openAPIDir is the package subdirectory holding the OpenAPI schemas.
	openAPIDir = "openapi"
	// valuesFile is the schema of the values the templates are rendered with. It
	// describes computed internal values rather than the settings a user writes.
	valuesFile = "values.yaml"
	// docRuPrefix marks the Russian translation of the schema whose name follows it.
	docRuPrefix = "doc-ru-"
	// testsSuffix marks the test fixtures a schema may ship alongside itself. A fixture
	// holds values rather than a schema, so no rule reads one.
	testsSuffix = "-tests.yaml"
)

// schemaNames returns the names of the schema files directly under packageDir/openapi,
// sorted so a directory always reports its findings in the same order. Only the top
// level is listed, because that is where the runtime reads a package's schemas from.
// An absent directory yields no names rather than an error: a package that ships no
// schemas is not a rule's concern.
func schemaNames(packageDir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(packageDir, openAPIDir))
	if os.IsNotExist(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(entries))

	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}

		if ext := filepath.Ext(entry.Name()); ext != ".yaml" && ext != ".yml" {
			continue
		}

		names = append(names, entry.Name())
	}

	slices.Sort(names)

	return names, nil
}

// isTranslation reports whether name is the Russian translation of another schema.
func isTranslation(name string) bool {
	return strings.HasPrefix(name, docRuPrefix)
}

// isTestFixture reports whether name holds schema test fixtures rather than a schema.
func isTestFixture(name string) bool {
	return strings.HasSuffix(name, testsSuffix)
}
