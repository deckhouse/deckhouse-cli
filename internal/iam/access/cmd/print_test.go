/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package access

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// printStructuredFixture intentionally uses only json tags (matching every
// real type in this package) so the test exercises the same JSON-tagged
// path that production code does.
type printStructuredFixture struct {
	Hello   string   `json:"hello"`
	Numbers []int    `json:"numbers"`
	Nested  []string `json:"nested"`
}

func TestPrintStructured_JSON(t *testing.T) {
	var buf bytes.Buffer
	err := printStructured(&buf, printStructuredFixture{
		Hello:   "world",
		Numbers: []int{1, 2, 3},
		Nested:  []string{},
	}, "json")
	require.NoError(t, err)

	want := `{
  "hello": "world",
  "numbers": [
    1,
    2,
    3
  ],
  "nested": []
}
`
	assert.Equal(t, want, buf.String(), "json output must keep 2-space indent + trailing newline")
}

// TestPrintStructured_YAMLTagsAreJSON locks down the JSON-tag-first behaviour:
// printStructured marshals via JSON then converts to YAML, so YAML keys are
// the json-tag names (camelCase here), not the Go field names.
func TestPrintStructured_YAMLTagsAreJSON(t *testing.T) {
	var buf bytes.Buffer
	err := printStructured(&buf, printStructuredFixture{
		Hello:   "world",
		Numbers: []int{1},
		Nested:  []string{"a"},
	}, "yaml")
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "hello: world", "yaml keys must come from json tags, not Go field names")
	assert.Contains(t, out, "numbers:")
	assert.Contains(t, out, "nested:")
	assert.NotContains(t, out, "Hello:", "Go field name must not leak into yaml output")
}

func TestPrintStructured_UnknownFormat(t *testing.T) {
	var buf bytes.Buffer
	err := printStructured(&buf, printStructuredFixture{}, "xml")
	require.Error(t, err)
	assert.True(t, errors.Is(err, errUnsupportedFormat),
		"caller code uses errors.Is(err, errUnsupportedFormat) for branching; do not break that contract")
	assert.Contains(t, err.Error(), `"xml"`, "error message must surface the offending format")
	assert.Empty(t, buf.String(), "unknown format must not write partial output")
}

// TestPrintStructured_NilValueIsNullJSON guards against accidentally treating
// a nil interface as an error. Callers may legitimately marshal a typed nil
// (e.g. an empty optional field) and we want them to get JSON "null" / YAML
// "null" rather than a hard error.
func TestPrintStructured_NilValueIsNullJSON(t *testing.T) {
	var buf bytes.Buffer
	err := printStructured(&buf, nil, "json")
	require.NoError(t, err)
	assert.Equal(t, "null\n", buf.String())
}

func TestDenil_NilSliceBecomesEmpty(t *testing.T) {
	got := denil[string](nil)
	require.NotNil(t, got, "denil must turn nil into a non-nil empty slice so JSON encodes []")
	assert.Equal(t, 0, len(got))
}

func TestDenil_NonNilSliceIsReturnedUnchanged(t *testing.T) {
	in := []int{1, 2, 3}
	got := denil(in)
	require.Equal(t, in, got)
	// Same backing array — denil never copies, since the only purpose is to
	// avoid the nil sentinel for JSON encoding.
	in[0] = 99
	assert.Equal(t, 99, got[0], "denil must not allocate a new slice for non-nil inputs")
}

// TestDenil_EmptyNonNilSliceStaysEmpty ensures we don't accidentally
// short-circuit on len == 0 (which would still allocate). The contract is
// purely about nil vs non-nil.
func TestDenil_EmptyNonNilSliceStaysEmpty(t *testing.T) {
	in := []string{}
	got := denil(in)
	require.NotNil(t, got)
	assert.Equal(t, 0, len(got))
}

// TestPrintStructured_GrantJSONShape sanity-checks that the package-level
// shared types (memberJSON, grantJSON, effectiveJSON, ...) round-trip
// through both formats with identical visible field names. This is the
// load-bearing assumption behind moving `iam access explain group -o json`
// off its private inline types onto the shared ones.
func TestPrintStructured_GrantJSONShape(t *testing.T) {
	g := grantJSON{
		AccessLevel: "Admin",
		Source: sourceJSON{
			Kind:      "ClusterAuthorizationRule",
			Name:      "demo",
			Namespace: "",
		},
		Scope:          scopeJSON{Type: "cluster"},
		AllowScale:     true,
		PortForwarding: false,
		ManagedByD8:    true,
	}

	var jsonBuf, yamlBuf bytes.Buffer
	require.NoError(t, printStructured(&jsonBuf, g, "json"))
	require.NoError(t, printStructured(&yamlBuf, g, "yaml"))

	jsonOut := jsonBuf.String()
	yamlOut := yamlBuf.String()

	for _, key := range []string{"accessLevel", "source", "scope", "allowScale", "portForwarding", "managedByD8"} {
		assert.Truef(t, strings.Contains(jsonOut, key), "json output missing key %q: %s", key, jsonOut)
		assert.Truef(t, strings.Contains(yamlOut, key), "yaml output missing key %q: %s", key, yamlOut)
	}
}
