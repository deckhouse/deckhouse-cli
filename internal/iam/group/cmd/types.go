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

package group

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// getGroupMembers extracts spec.members from a Group object.
func getGroupMembers(obj *unstructured.Unstructured) ([]map[string]any, error) {
	raw, found, err := unstructured.NestedSlice(obj.Object, "spec", "members")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	result := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		result = append(result, m)
	}
	return result, nil
}
