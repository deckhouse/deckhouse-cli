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
	"encoding/json"
	"errors"
	"fmt"
	"io"

	sigsyaml "sigs.k8s.io/yaml"
)

// errUnsupportedFormat is returned for an unknown -o value. Callers wrap it
// with the per-command list of accepted formats; tests use errors.Is.
var errUnsupportedFormat = errors.New("unsupported output format")

// printStructured renders v as JSON or YAML. YAML goes through json.Marshal +
// sigsyaml.JSONToYAML so json tags drive field names — our typed shapes
// declare only json tags. For Kubernetes manifests use utilk8s.PrintObject
// instead, which preserves the apiVersion/kind layout.
func printStructured(w io.Writer, v any, format string) error {
	switch format {
	case "json":
		data, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Errorf("marshalling JSON: %w", err)
		}
		_, err = fmt.Fprintln(w, string(data))
		return err
	case "yaml":
		jsonData, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("marshalling JSON for YAML conversion: %w", err)
		}
		yamlData, err := sigsyaml.JSONToYAML(jsonData)
		if err != nil {
			return fmt.Errorf("converting JSON to YAML: %w", err)
		}
		_, err = fmt.Fprint(w, string(yamlData))
		return err
	default:
		return fmt.Errorf("%w %q", errUnsupportedFormat, format)
	}
}

// denil turns a nil slice into an empty one so JSON emits "[]" instead of
// "null". Returned by value to keep call sites one expression.
func denil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
