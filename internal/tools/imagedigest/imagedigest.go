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

package imagedigest

import (
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/crane"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"go.cypherpunks.ru/gogost/v5/gost34112012256"
)

const (
	GostDigestAnnotationKey = "deckhouse.io/gost-digest"
)

// ValidationResult contains the result of GOST digest validation.
type ValidationResult struct {
	StoredDigest     string // Digest read from image annotation
	CalculatedDigest string // Freshly calculated digest from layers
}

// ImageInfo contains display information about an image (read-only DTO).
type ImageInfo struct {
	Name         string
	Digest       string
	GostDigest   string   // May be empty if not annotated
	LayerDigests []string // Sorted
}

// =============================================================================
// Layer 1: Pure Hash Computation (no dependencies on image types)
// =============================================================================

// CalculateGostHash computes GOST R 34.11-2012 (Streebog-256) from raw bytes.
func CalculateGostHash(data []byte) []byte {
	hasher := gost34112012256.New()
	hasher.Write(data)
	return hasher.Sum(nil)
}

// CalculateGostHashFromReader computes GOST R 34.11-2012 hash from io.Reader.
func CalculateGostHashFromReader(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return CalculateGostHash(data), nil
}

// =============================================================================
// Layer 2: Layer Digest Extraction (depends only on v1.Image)
// =============================================================================

// ExtractSortedLayerDigests returns sorted layer digests from an image.
func ExtractSortedLayerDigests(image v1.Image) ([]string, error) {
	layers, err := image.Layers()
	if err != nil {
		return nil, err
	}

	digests := make([]string, 0, len(layers))
	for _, layer := range layers {
		digest, err := layer.Digest()
		if err != nil {
			return nil, err
		}
		digests = append(digests, digest.String())
	}

	slices.Sort(digests)
	return digests, nil
}

// ReadGostAnnotation reads the GOST digest annotation if present.
// Returns the digest value, whether it was found, and any error.
func ReadGostAnnotation(image v1.Image) (string, bool, error) {
	manifest, err := image.Manifest()
	if err != nil {
		return "", false, err
	}

	digest, ok := manifest.Annotations[GostDigestAnnotationKey]
	return digest, ok, nil
}

// AddGostAnnotation returns a new image with the GOST annotation added.
func AddGostAnnotation(image v1.Image, digestHex string) v1.Image {
	return mutate.Annotations(image, map[string]string{
		GostDigestAnnotationKey: digestHex,
	}).(v1.Image)
}

// =============================================================================
// Layer 3: Composed Operations (combines layers 1 and 2)
// =============================================================================

// CalculateImageGostDigest calculates GOST digest from an image's layers.
func CalculateImageGostDigest(image v1.Image) ([]byte, error) {
	digests, err := ExtractSortedLayerDigests(image)
	if err != nil {
		return nil, err
	}
	if len(digests) == 0 {
		return nil, fmt.Errorf("invalid layers hash data: no layers found")
	}
	return CalculateGostHash([]byte(strings.Join(digests, ""))), nil
}

// AnnotateWithGostDigest calculates GOST digest and returns annotated image.
// Returns the annotated image and the calculated digest in hex format.
func AnnotateWithGostDigest(image v1.Image) (v1.Image, string, error) {
	digest, err := CalculateImageGostDigest(image)
	if err != nil {
		return nil, "", err
	}
	digestHex := hex.EncodeToString(digest)
	return AddGostAnnotation(image, digestHex), digestHex, nil
}

// ValidateGostDigest verifies the stored annotation matches recalculated digest.
// Returns ValidationResult with both digests for comparison, and error if mismatch.
func ValidateGostDigest(image v1.Image) (*ValidationResult, error) {
	stored, ok, err := ReadGostAnnotation(image)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("image does not contain GOST digest annotation (%s)", GostDigestAnnotationKey)
	}

	calculated, err := CalculateImageGostDigest(image)
	if err != nil {
		return &ValidationResult{StoredDigest: stored}, err
	}

	result := &ValidationResult{
		StoredDigest:     stored,
		CalculatedDigest: hex.EncodeToString(calculated),
	}

	if err := compareDigests(stored, calculated); err != nil {
		return result, err
	}
	return result, nil
}

// compareDigests compares stored hex digest with calculated hash using constant-time comparison.
func compareDigests(storedHex string, calculatedHash []byte) error {
	storedBytes, err := hex.DecodeString(storedHex)
	if err != nil {
		return fmt.Errorf("invalid stored GOST digest format: %w", err)
	}

	if subtle.ConstantTimeCompare(storedBytes, calculatedHash) == 0 {
		return fmt.Errorf("GOST digest mismatch: stored=%s, calculated=%s",
			storedHex, hex.EncodeToString(calculatedHash))
	}
	return nil
}

// =============================================================================
// Layer 4: Registry Operations (high-level convenience with network I/O)
// =============================================================================

// PullAndCalculate pulls image and calculates GOST digest.
func PullAndCalculate(imageName string, opts ...crane.Option) ([]byte, error) {
	image, err := crane.Pull(imageName, opts...)
	if err != nil {
		return nil, err
	}
	return CalculateImageGostDigest(image)
}

// PullAnnotatePush pulls image, annotates with GOST digest, and pushes back.
// Returns the calculated digest in hex format.
func PullAnnotatePush(imageName string, opts ...crane.Option) (string, error) {
	image, err := crane.Pull(imageName, opts...)
	if err != nil {
		return "", err
	}

	annotated, digestHex, err := AnnotateWithGostDigest(image)
	if err != nil {
		return "", err
	}

	if err := crane.Push(annotated, imageName, opts...); err != nil {
		return "", err
	}
	return digestHex, nil
}

// PullAndValidate pulls image and validates its GOST digest.
func PullAndValidate(imageName string, opts ...crane.Option) (*ValidationResult, error) {
	image, err := crane.Pull(imageName, opts...)
	if err != nil {
		return nil, err
	}
	return ValidateGostDigest(image)
}

// =============================================================================
// Utility: ImageInfo for display purposes
// =============================================================================

// GetImageInfo extracts display information from an image.
func GetImageInfo(name string, image v1.Image) (*ImageInfo, error) {
	info := &ImageInfo{Name: name}

	imageDigest, err := image.Digest()
	if err != nil {
		return nil, err
	}
	info.Digest = imageDigest.String()

	gostDigest, ok, err := ReadGostAnnotation(image)
	if err != nil {
		return nil, err
	}
	if ok {
		info.GostDigest = gostDigest
	}

	layerDigests, err := ExtractSortedLayerDigests(image)
	if err != nil {
		return nil, err
	}
	info.LayerDigests = layerDigests

	return info, nil
}

