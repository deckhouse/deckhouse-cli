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

// errUnsupportedFormat is returned by printStructured when the format
// argument is neither "json" nor "yaml". Callers in this package wrap it
// with the per-command list of accepted values, e.g.:
//
//	return fmt.Errorf("%w; use table|json|yaml", err)
//
// Tests assert via errors.Is.
var errUnsupportedFormat = errors.New("unsupported output format")

// printStructured renders v as JSON or YAML on w.
//
// JSON path uses MarshalIndent with two-space indent and a trailing
// newline (via Fprintln). The historic call sites in this package emitted
// exactly that, so swapping them to printStructured stays byte-identical
// for scripted consumers.
//
// YAML path goes JSON -> sigsyaml.JSONToYAML so json tags drive field
// names. Every typed shape we print here (grantJSON, ruleJSON,
// userAccessJSON, ...) declares only json tags; dual-tagging every struct
// purely for one print path would be maintenance cost without benefit.
// utilk8s.PrintObject takes the same JSON-then-YAML approach for
// Unstructured manifests, keeping the two helpers symmetric.
//
// For Kubernetes manifests (user/group/grant create -o yaml) keep using
// utilk8s.PrintObject — it preserves the apiVersion/kind layout and
// supports the "name" format that is meaningful only for k8s objects.
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

// denil normalises a nil slice to an empty (but non-nil) slice so JSON
// encoding emits "[]" instead of "null". Every iam list/get JSON payload
// in this package promises that contract so machine consumers can iterate
// the field unconditionally.
//
// Returned by value rather than mutating through a pointer because every
// call site in this package already assigns the field with the result of
// a build step — chaining with denil keeps the call site one expression
// instead of two statements.
func denil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
