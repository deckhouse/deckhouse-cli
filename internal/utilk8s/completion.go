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
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// CompleteResourceNames lists names of objects of the given GVR for shell
// completion. Pass namespace="" for cluster-scoped resources or to list across
// the default namespace for namespaced resources.
//
// Returns ShellCompDirectiveError if the cluster is unreachable; cobra renders
// that as "no completions" in the shell rather than failing the command.
func CompleteResourceNames(cmd *cobra.Command, gvr schema.GroupVersionResource, namespace, toComplete string) ([]string, cobra.ShellCompDirective) {
	dyn, err := NewDynamicClient(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	ri := dyn.Resource(gvr)
	list, err := ri.List(cmd.Context(), metav1.ListOptions{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		n := list.Items[i].GetName()
		if strings.HasPrefix(n, toComplete) {
			names = append(names, n)
		}
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}

// CompleteNamespaces returns namespace names for shell completion.
func CompleteNamespaces(cmd *cobra.Command, toComplete string) ([]string, cobra.ShellCompDirective) {
	return CompleteResourceNames(cmd, schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, "", toComplete)
}

// FilterByPrefix returns elements of values that start with toComplete.
// Convenient for static enum completions.
func FilterByPrefix(values []string, toComplete string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if strings.HasPrefix(v, toComplete) {
			out = append(out, v)
		}
	}
	return out
}

// CompleteOutputFormats returns a cobra completion function that suggests a
// static list of output format names (e.g. "table", "json", "yaml") filtered
// by the current toComplete prefix. The returned function is safe to pass
// directly to cmd.RegisterFlagCompletionFunc.
func CompleteOutputFormats(formats ...string) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return FilterByPrefix(formats, toComplete), cobra.ShellCompDirectiveNoFileComp
	}
}
