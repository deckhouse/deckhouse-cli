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

// Package vextest builds VEX attestations in the shapes delivery-kit
// publishes, for tests of the mirror.
package vextest

import (
	"fmt"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const (
	// BundleMediaType is the artifactType of a signed attestation.
	BundleMediaType = "application/vnd.dev.sigstore.bundle.v0.3+json"
	// DSSEMediaType is the layer media type of a legacy attestation.
	DSSEMediaType = "application/vnd.dsse.envelope.v1+json"
	// PredicateTypeAnnotation names the predicate type of a referrers artifact.
	PredicateTypeAnnotation = "dev.sigstore.bundle.predicateType"
	// OpenVEXPredicateType is the predicate type of a signed OpenVEX attestation.
	OpenVEXPredicateType = "https://openvex.dev/ns"
)

// LegacyAttestation returns a cosign-style attestation image, stored under
// sha256-<hex>.att: one DSSE envelope layer with the predicate type annotated.
func LegacyAttestation(payload string) (v1.Image, error) {
	img, err := mutate.Append(empty.Image, mutate.Addendum{
		Layer:       static.NewLayer([]byte(payload), DSSEMediaType),
		MediaType:   DSSEMediaType,
		Annotations: map[string]string{"predicateType": OpenVEXPredicateType},
	})
	if err != nil {
		return nil, fmt.Errorf("append DSSE layer: %w", err)
	}

	img = mutate.MediaType(img, types.OCIManifestSchema1)

	return mutate.ConfigMediaType(img, types.OCIConfigJSON), nil
}

// ReferrersIndex returns the referrers index stored under sha256-<hex> of
// subject: it lists one signed OpenVEX artifact whose subject is subject,
// with the artifactType and annotations by which consumers find it.
func ReferrersIndex(subject v1.Image, payload string) (v1.ImageIndex, error) {
	annotations := map[string]string{
		PredicateTypeAnnotation: OpenVEXPredicateType,
		"io.werf.image-name":    "test/image",
	}

	artifact, err := mutate.AppendLayers(empty.Image, static.NewLayer([]byte(payload), BundleMediaType))
	if err != nil {
		return nil, fmt.Errorf("append bundle layer: %w", err)
	}

	artifact = mutate.MediaType(artifact, types.OCIManifestSchema1)
	artifact = mutate.ConfigMediaType(artifact, BundleMediaType)

	artifact, ok := mutate.Annotations(artifact, annotations).(v1.Image)
	if !ok {
		return nil, fmt.Errorf("annotate artifact: unexpected mutate result type")
	}

	subjectDesc, err := partial.Descriptor(subject)
	if err != nil {
		return nil, fmt.Errorf("describe subject: %w", err)
	}

	artifact, ok = mutate.Subject(artifact, *subjectDesc).(v1.Image)
	if !ok {
		return nil, fmt.Errorf("set artifact subject: unexpected mutate result type")
	}

	// The descriptor gets its artifactType from the config media type.
	return mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
		Add:        artifact,
		Descriptor: v1.Descriptor{Annotations: annotations},
	}), nil
}
