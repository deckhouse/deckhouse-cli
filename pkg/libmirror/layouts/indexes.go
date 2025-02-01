/*
Copyright 2025 Flant JSC

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

package layouts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"golang.org/x/exp/maps"
)

// ociIndexManifest represents an OCI image index.
type ociIndexManifest struct {
	SchemaVersion int64                    `json:"schemaVersion"`
	MediaType     types.MediaType          `json:"mediaType,omitempty"`
	Manifests     []v1.Descriptor          `json:"manifests"`
	Annotations   indexManifestAnnotations `json:"annotations,omitempty"`
	Subject       *v1.Descriptor           `json:"subject,omitempty"`
}

// indexManifestAnnotations is a map of image annotations that marshals to JSON form while keeping keys ordered alphabetically.
type indexManifestAnnotations map[string]string

// MarshalJSON marshals go map while keeping keys ordered alphabetically in resulting JSON.
func (a indexManifestAnnotations) MarshalJSON() ([]byte, error) {
	names := maps.Keys(a)
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteRune('{')
	for _, key := range names {
		buf.WriteRune('"')
		buf.WriteString(key)
		buf.Write([]byte(`": "`))
		buf.WriteString(a[key])
		buf.Write([]byte(`",`))
	}
	js := buf.Bytes()
	js[len(js)-1] = '}'
	return js, nil
}

func SortIndexManifests(l layout.Path) error {
	index, err := l.ImageIndex()
	if err != nil {
		return fmt.Errorf("Read image index: %w", err)
	}

	rawManifest, err := index.RawManifest()
	if err != nil {
		return fmt.Errorf("Read index manifest: %w", err)
	}

	indexManifest := &ociIndexManifest{}
	if err = json.Unmarshal(rawManifest, indexManifest); err != nil {
		return fmt.Errorf("Parse index manifest: %w", err)
	}

	sort.Slice(indexManifest.Manifests, func(i, j int) bool {
		ref1 := indexManifest.Manifests[i].Annotations["org.opencontainers.image.ref.name"]
		ref2 := indexManifest.Manifests[j].Annotations["org.opencontainers.image.ref.name"]
		return ref1 < ref2
	})

	rawManifest, err = json.MarshalIndent(indexManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("Marshal image index manifest: %w", err)
	}
	if err = l.WriteFile("index.json", rawManifest, os.ModePerm); err != nil {
		return fmt.Errorf("Write image index manifest: %w", err)
	}

	return nil
}
