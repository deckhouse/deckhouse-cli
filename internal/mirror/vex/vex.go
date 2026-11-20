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

	"github.com/deckhouse/deckhouse/pkg/registry"
)

const (
	// LegacyTagSuffix ends the tag of a legacy (cosign-style) VEX attestation.
	LegacyTagSuffix = ".att"

	// referrersTagPrefix starts the tag of a referrers index, sha256-<hex>.
	referrersTagPrefix = "sha256-"

	sha256HexLength = 64
)

// Checker is the registry call Find needs; pkg.BasicService satisfies it.
type Checker interface {
	CheckImageExists(ctx context.Context, tag string) error
}

// Ref is a VEX attestation found for an image.
type Ref struct {
	// Name is the full reference, <repository>:<tag>.
	Name string
	// Index tells that Name is a referrers index, to be pulled whole, rather
	// than a single attestation image.
	Index bool
}

// Find returns the VEX attestations of imageRef, the legacy one first. c must
// be scoped to the repository the attestations are looked up in. A tag the
// registry does not have is skipped; any other error fails the lookup.
func Find(ctx context.Context, c Checker, imageRef string) ([]Ref, error) {
	candidates := []Ref{{Name: LegacyRef(imageRef)}}
	if ref, ok := ReferrersRef(imageRef); ok {
		candidates = append(candidates, Ref{Name: ref, Index: true})
	}

	found := make([]Ref, 0, len(candidates))

	for _, candidate := range candidates {
		tag := candidate.Name[strings.LastIndex(candidate.Name, ":")+1:]

		err := c.CheckImageExists(ctx, tag)
		if errors.Is(err, registry.ErrImageNotFound) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("check VEX attestation %s: %w", candidate.Name, err)
		}

		found = append(found, candidate)
	}

	return found, nil
}

// LegacyRef returns the reference of the legacy VEX attestation of imageRef:
// <repo>:sha256-<hex>.att for <repo>@sha256:<hex>, <repo>:<tag>.att for
// <repo>:<tag>.
func LegacyRef(imageRef string) string {
	if repo, hex, ok := strings.Cut(imageRef, "@sha256:"); ok {
		return repo + ":" + referrersTagPrefix + hex + LegacyTagSuffix
	}

	return imageRef + LegacyTagSuffix
}

// ReferrersRef returns the reference of the referrers index of imageRef,
// <repo>:sha256-<hex> for <repo>@sha256:<hex>. The tag schema is keyed by
// digest, so a reference by tag has no such index and ok is false.
func ReferrersRef(imageRef string) (string, bool) {
	repo, hex, ok := strings.Cut(imageRef, "@sha256:")
	if !ok {
		return "", false
	}

	return repo + ":" + referrersTagPrefix + hex, true
}

// IsTag reports whether tag names a VEX attestation in either format.
func IsTag(tag string) bool {
	return strings.HasSuffix(tag, LegacyTagSuffix) || isReferrersTag(tag)
}

func isReferrersTag(tag string) bool {
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
