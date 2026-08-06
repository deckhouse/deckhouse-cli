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

package source

import (
	"encoding/json"
	"fmt"
)

// SourceRefIdentity is the decoded form of the state-snapshotter.deckhouse.io/source-ref annotation.
// The annotation value is a JSON object {apiVersion, kind, namespace, name, uid} matching
// the SnapshotSourceIdentity type in state-snapshotter common/source_ref_annotation.go.
type SourceRefIdentity struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}

// ParseSourceRef decodes the raw source-ref annotation string into a SourceRefIdentity.
// An empty or malformed annotation is not fatal: callers that only need the Name field
// may ignore the returned error. On any parse failure the zero SourceRefIdentity is returned.
func ParseSourceRef(raw string) (SourceRefIdentity, error) {
	if raw == "" {
		return SourceRefIdentity{}, fmt.Errorf("source-ref annotation is empty")
	}

	var id SourceRefIdentity
	if err := json.Unmarshal([]byte(raw), &id); err != nil {
		return SourceRefIdentity{}, fmt.Errorf("parse source-ref annotation: %w", err)
	}

	return id, nil
}
