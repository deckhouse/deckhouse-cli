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

// Package vex finds the VEX attestations that the mirror pulls along with the
// images they describe. delivery-kit has stored them in two formats, and a
// source registry can hold either one or both for the same image:
//
//   - legacy: a cosign-style attestation manifest tagged sha256-<hex>.att,
//     pulled as a single image;
//   - referrers: an OCI artifact pushed untagged with its subject set to the
//     image and listed in an OCI image index tagged sha256-<hex> (the
//     referrers tag schema of the OCI distribution spec). The index is pulled
//     whole and byte for byte: consumers find the attestation by the
//     artifactType and annotations of its descriptor in that index.
//
// Both formats are pulled, so consumers that read only .att keep working.
package vex

import (
	"context"
	"errors"
	"fmt"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/deckhouse/deckhouse/pkg/registry"
)

const (
	// LegacyTagSuffix ends the tag of a legacy (cosign-style) VEX attestation.
	LegacyTagSuffix = ".att"

	// referrersTagPrefix starts the tag of a referrers index, sha256-<hex>.
	referrersTagPrefix = "sha256-"

	sha256HexLength = 64
)

// Resolver reads a tag's manifest digest in the subject repository.
type Resolver interface {
	GetDigest(ctx context.Context, tag string) (*v1.Hash, error)
}

// Ref is an attestation tag and its resolved manifest digest.
type Ref struct {
	Name   string
	Digest *v1.Hash
	Index  bool
}

// Find looks up attestations for the exact digest selected by the pull.
// A tag-addressed subject can also have an attestation under <tag>.att.
func Find(ctx context.Context, resolver Resolver, imageRef string, digest v1.Hash) ([]Ref, error) {
	return find(ctx, resolver, imageRef, digest, true)
}

// FindByDigest searches the two digest tags in the subject repository.
func FindByDigest(ctx context.Context, resolver Resolver, imageRef string, digest v1.Hash) ([]Ref, error) {
	return find(ctx, resolver, imageRef, digest, false)
}

func find(ctx context.Context, resolver Resolver, imageRef string, digest v1.Hash, includeTag bool) ([]Ref, error) {
	repo, _, byDigest := strings.Cut(imageRef, "@")
	if !byDigest {
		separator := strings.LastIndexByte(imageRef, ':')
		if separator <= strings.LastIndexByte(imageRef, '/') {
			return nil, fmt.Errorf("missing tag in VEX subject reference %q", imageRef)
		}

		repo = imageRef[:separator]
	}

	indexRef := repo + ":" + digest.Algorithm + "-" + digest.Hex
	legacyRef := indexRef + LegacyTagSuffix

	var candidates []Ref
	if includeTag && !byDigest && imageRef+LegacyTagSuffix != legacyRef {
		candidates = append(candidates, Ref{Name: imageRef + LegacyTagSuffix})
	}

	candidates = append(candidates, Ref{Name: legacyRef}, Ref{Name: indexRef, Index: true})

	found := make([]Ref, 0, len(candidates))
	for _, candidate := range candidates {
		tag := strings.TrimPrefix(candidate.Name, repo+":")

		hash, err := resolver.GetDigest(ctx, tag)
		if errors.Is(err, registry.ErrImageNotFound) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("resolve attestation %s: %w", candidate.Name, err)
		}

		candidate.Digest = hash
		found = append(found, candidate)
	}

	return found, nil
}

// IsTag reports whether tag names a VEX attestation in either format.
func IsTag(tag string) bool {
	return strings.HasSuffix(tag, LegacyTagSuffix) || IsReferrersTag(tag)
}

// IsReferrersTag matches the digest tag of a referrers index.
func IsReferrersTag(tag string) bool {
	hex, ok := strings.CutPrefix(tag, referrersTagPrefix)
	if !ok || len(hex) != sha256HexLength {
		return false
	}

	for _, r := range hex {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}

	return true
}
