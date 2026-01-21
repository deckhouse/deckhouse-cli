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
	"sort"
	"strings"

	"github.com/google/go-containerregistry/pkg/crane"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"go.cypherpunks.ru/gogost/v5/gost34112012256"
)

const (
	GostDigestAnnotationKey = "deckhouse.io/gost-digest"
)

type ImageMetadata struct {
	ImageName       string
	ImageDigest     string
	ImageGostDigest string
	LayersDigest    []string
}

// ValidationResult contains the result of GOST digest validation.
type ValidationResult struct {
	StoredDigest     string // Digest read from image annotation
	CalculatedDigest string // Freshly calculated digest from layers
}

// CalculateGostImageDigest calculates GOST R 34.11-2012 (Streebog-256) digest
// for a container image based on sorted layer digests.
func CalculateGostImageDigest(imageName string, opts ...crane.Option) ([]byte, error) {
	im, err := getImageMetadataFromRegistry(imageName, opts...)
	if err != nil {
		return nil, err
	}

	return calculateLayersGostDigest(im)
}

// AddGostImageDigest calculates and adds GOST digest to image annotations.
func AddGostImageDigest(imageName string, opts ...crane.Option) (string, error) {
	image, err := getImageFromRegistry(imageName, opts...)
	if err != nil {
		return "", err
	}
	im, err := imageToImageMetadata(imageName, image)
	if err != nil {
		return "", err
	}

	gostImageDigest, err := calculateLayersGostDigest(im)
	if err != nil {
		return "", err
	}

	digestHex := hex.EncodeToString(gostImageDigest)

	err = updateImageInRegistry(
		imageName,
		image,
		map[string]string{
			GostDigestAnnotationKey: digestHex,
		},
		opts...,
	)
	if err != nil {
		return "", err
	}

	return digestHex, nil
}

// ValidateGostImageDigest validates stored GOST digest against recalculated digest.
func ValidateGostImageDigest(imageName string, opts ...crane.Option) (*ValidationResult, error) {
	im, err := getImageMetadataFromRegistry(imageName, opts...)
	if err != nil {
		return nil, err
	}

	if len(im.ImageGostDigest) == 0 {
		return nil, fmt.Errorf("image %s does not contain GOST digest annotation (%s)", imageName, GostDigestAnnotationKey)
	}

	result := &ValidationResult{
		StoredDigest: im.ImageGostDigest,
	}

	gostImageDigest, err := calculateLayersGostDigest(im)
	if err != nil {
		return result, err
	}
	result.CalculatedDigest = hex.EncodeToString(gostImageDigest)

	err = compareImageGostHash(im, gostImageDigest)
	if err != nil {
		return result, err
	}

	return result, nil
}

func getImageMetadataFromRegistry(imageName string, opts ...crane.Option) (*ImageMetadata, error) {
	image, err := getImageFromRegistry(imageName, opts...)
	if err != nil {
		return nil, err
	}
	return imageToImageMetadata(imageName, image)
}

func getImageFromRegistry(imageName string, opts ...crane.Option) (v1.Image, error) {
	return crane.Pull(imageName, opts...)
}

func updateImageInRegistry(
	imageName string,
	image v1.Image,
	annotations map[string]string,
	opts ...crane.Option,
) error {
	image = mutate.Annotations(image, annotations).(v1.Image)
	return crane.Push(image, imageName, opts...)
}

func imageToImageMetadata(imageName string, image v1.Image) (*ImageMetadata, error) {
	im := &ImageMetadata{ImageName: imageName}

	imageDigest, err := image.Digest()
	if err != nil {
		return nil, err
	}
	im.ImageDigest = imageDigest.String()

	manifest, err := image.Manifest()
	if err != nil {
		return nil, err
	}

	imageGostDigestStr, ok := manifest.Annotations[GostDigestAnnotationKey]
	if ok {
		im.ImageGostDigest = imageGostDigestStr
	}

	layers, err := image.Layers()
	if err != nil {
		return nil, err
	}

	for _, layer := range layers {
		digest, err := layer.Digest()
		if err != nil {
			return nil, err
		}
		im.LayersDigest = append(im.LayersDigest, digest.String())
	}

	sort.Slice(
		im.LayersDigest,
		func(i, j int) bool {
			return strings.Compare(im.LayersDigest[i], im.LayersDigest[j]) == -1
		},
	)

	return im, nil
}

func calculateLayersGostDigest(im *ImageMetadata) ([]byte, error) {
	layersDigestBuilder := strings.Builder{}
	for _, digest := range im.LayersDigest {
		layersDigestBuilder.WriteString(digest)
	}

	data := layersDigestBuilder.String()

	if len(data) == 0 {
		return nil, fmt.Errorf("invalid layers hash data: no layers found")
	}

	hasher := gost34112012256.New()
	_, err := hasher.Write([]byte(data))
	if err != nil {
		return nil, err
	}

	return hasher.Sum(nil), nil
}

func compareImageGostHash(im *ImageMetadata, gostHash []byte) error {
	imageGostHashByte, err := hex.DecodeString(im.ImageGostDigest)
	if err != nil {
		return fmt.Errorf("invalid stored GOST digest format: %w", err)
	}

	if subtle.ConstantTimeCompare(imageGostHashByte, gostHash) == 0 {
		return fmt.Errorf("GOST digest mismatch: stored=%s, calculated=%s",
			im.ImageGostDigest, hex.EncodeToString(gostHash))
	}
	return nil
}

// CalculateFromReader calculates GOST R 34.11-2012 (Streebog-256) digest
// from an io.Reader (file or stdin).
func CalculateFromReader(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	hasher := gost34112012256.New()
	_, err = hasher.Write(data)
	if err != nil {
		return nil, err
	}

	return hasher.Sum(nil), nil
}
