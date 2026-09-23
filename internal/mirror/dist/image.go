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

package dist

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/types"

	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/pkg"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// Every pull in this package is retried on transport errors: a bundle is
// often built over a long, flaky link, and losing an hour of downloads to one
// dropped connection is the failure mode worth spending retries on.
const (
	pullRetryAttempts = 5
	pullRetryDelay    = 10 * time.Second
)

// imageSource is the slice of a registry service that pulling one tag needs.
// Both the plugin repositories and the deckhouse-cli repository satisfy it, so
// the CLI binary and the plugins travel the same code path.
type imageSource interface {
	GetManifest(ctx context.Context, tag string) (dkpreg.ManifestResult, error)
	GetImage(ctx context.Context, tag string, opts ...dkpreg.ImageGetOption) (pkg.RegistryImage, error)
}

// pullTag pulls one tag into an OCI layout. A multi-platform index is stored
// whole: children are fetched by digest, so their bytes (and any annotation
// riding on them, such as a plugin contract) stay exactly as published. ref is
// the full registry reference, recorded in the layout as the tag's origin.
func pullTag(ctx context.Context, src imageSource, dest *regimage.ImageLayout, tag, ref string) error {
	result, err := src.GetManifest(ctx, tag)
	if err != nil {
		return fmt.Errorf("get manifest: %w", err)
	}

	if !result.GetMediaType().IsIndex() {
		img, err := src.GetImage(ctx, tag)
		if err != nil {
			return fmt.Errorf("get image: %w", err)
		}

		return dest.AddImage(img, tag)
	}

	indexManifest, err := result.GetIndexManifest()
	if err != nil {
		return fmt.Errorf("read index manifest: %w", err)
	}

	idx, err := rebuildIndex(ctx, src, indexManifest, result.GetMediaType())
	if err != nil {
		return err
	}

	return dest.AddIndex(idx, tag, ref)
}

// rebuildIndex reassembles a multi-platform index from its children. Children
// are fetched by digest (byte-exact); only the top-level index manifest is
// re-marshaled locally, with its media type, annotations, subject and the
// per-child descriptor fields carried over. Per-child artifactType is the one
// field ggcr's mutate cannot carry.
func rebuildIndex(ctx context.Context, src imageSource, indexManifest dkpreg.IndexManifest, mediaType types.MediaType) (v1.ImageIndex, error) {
	children := indexManifest.GetManifests()

	adds := make([]mutate.IndexAddendum, 0, len(children))

	for _, child := range children {
		img, err := src.GetImage(ctx, "@"+child.GetDigest().String())
		if err != nil {
			return nil, fmt.Errorf("get platform image %s: %w", child.GetDigest(), err)
		}

		adds = append(adds, mutate.IndexAddendum{
			Add: img,
			Descriptor: v1.Descriptor{
				MediaType:   child.GetMediaType(),
				URLs:        child.GetURLs(),
				Annotations: child.GetAnnotations(),
				Platform:    child.GetPlatform(),
				Data:        child.GetData(),
			},
		})
	}

	idx := mutate.AppendManifests(empty.Index, adds...)

	if annotations := indexManifest.GetAnnotations(); len(annotations) > 0 {
		annotated, ok := mutate.Annotations(idx, annotations).(v1.ImageIndex)
		if !ok {
			return nil, fmt.Errorf("annotate rebuilt index: unexpected mutate result type")
		}

		idx = annotated
	}

	idx = mutate.IndexMediaType(idx, mediaType)

	// Subject goes on last: every mutate wrapper rewrites the subject from
	// its own field, so a wrapper added later would erase it.
	if subject := indexManifest.GetSubject(); subject != nil {
		withSubject, ok := mutate.Subject(idx, v1.Descriptor{
			MediaType:    subject.GetMediaType(),
			Size:         subject.GetSize(),
			Digest:       subject.GetDigest(),
			Annotations:  subject.GetAnnotations(),
			ArtifactType: subject.GetArtifactType(),
		}).(v1.ImageIndex)
		if !ok {
			return nil, fmt.Errorf("set subject on rebuilt index: unexpected mutate result type")
		}

		idx = withSubject
	}

	return idx, nil
}
