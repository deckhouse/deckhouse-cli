/*
Copyright 2024 Flant JSC

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

package utilk8s

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	sigsyaml "sigs.k8s.io/yaml"
)

// AddOutputFlag declares the standard "-o/--output" flag and its completion
// in a single line. Default value and accepted formats are command-specific
// so callers list them explicitly.
func AddOutputFlag(cmd *cobra.Command, defaultFmt string, formats ...string) {
	cmd.Flags().StringP("output", "o", defaultFmt, "Output format: "+strings.Join(formats, "|"))
	_ = cmd.RegisterFlagCompletionFunc("output", CompleteOutputFormats(formats...))
}

// PrintObject writes an unstructured Kubernetes object to w in the given format.
// Supported formats: "json", "yaml"; anything else prints Kind/Name.
func PrintObject(w io.Writer, obj *unstructured.Unstructured, format string) error {
	switch format {
	case "json":
		data, err := json.MarshalIndent(obj.Object, "", "  ")
		if err != nil {
			return fmt.Errorf("marshalling JSON: %w", err)
		}
		fmt.Fprintln(w, string(data))
	case "yaml":
		data, err := sigsyaml.Marshal(obj.Object)
		if err != nil {
			return fmt.Errorf("marshalling YAML: %w", err)
		}
		fmt.Fprint(w, string(data))
	default:
		fmt.Fprintf(w, "%s/%s\n", obj.GetKind(), obj.GetName())
	}
	return nil
}
