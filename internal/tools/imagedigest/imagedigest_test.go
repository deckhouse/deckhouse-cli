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
	"encoding/hex"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
)

func TestCalculateLayersGostDigest(t *testing.T) {
	t.Run("empty layers error", func(t *testing.T) {
		_, err := CalculateLayersGostDigest(&ImageMetadata{})
		if err == nil || !strings.Contains(err.Error(), "no layers found") {
			t.Errorf("expected 'no layers found' error, got %v", err)
		}
	})
}

func TestCompareImageGostHash(t *testing.T) {
	hash, _ := CalculateLayersGostDigest(&ImageMetadata{LayersDigest: []string{"sha256:test"}})
	hashHex := hex.EncodeToString(hash)

	t.Run("matching", func(t *testing.T) {
		err := CompareImageGostHash(&ImageMetadata{ImageGostDigest: hashHex}, hash)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		err := CompareImageGostHash(&ImageMetadata{ImageGostDigest: hashHex}, make([]byte, 32))
		if err == nil || !strings.Contains(err.Error(), "mismatch") {
			t.Errorf("expected mismatch error, got %v", err)
		}
	})

	t.Run("invalid hex", func(t *testing.T) {
		err := CompareImageGostHash(&ImageMetadata{ImageGostDigest: "not-hex"}, hash)
		if err == nil || !strings.Contains(err.Error(), "invalid") {
			t.Errorf("expected invalid format error, got %v", err)
		}
	})
}

func TestImageToImageMetadata(t *testing.T) {
	img, err := random.Image(256, 2) // 256 bytes, 2 layers
	if err != nil {
		t.Fatalf("failed to create test image: %v", err)
	}

	t.Run("extracts metadata", func(t *testing.T) {
		im, err := ImageToImageMetadata("test:v1", img)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if im.ImageName != "test:v1" {
			t.Errorf("ImageName = %s, want test:v1", im.ImageName)
		}
		if im.ImageDigest == "" {
			t.Error("ImageDigest is empty")
		}
		if len(im.LayersDigest) != 2 {
			t.Errorf("LayersDigest length = %d, want 2", len(im.LayersDigest))
		}
	})

	t.Run("layers are sorted", func(t *testing.T) {
		im, _ := ImageToImageMetadata("test", img)
		for i := 1; i < len(im.LayersDigest); i++ {
			if im.LayersDigest[i-1] > im.LayersDigest[i] {
				t.Error("layers not sorted")
			}
		}
	})
}

func TestCalculateGostDigestFromImage(t *testing.T) {
	img, _ := random.Image(256, 2)

	t.Run("success", func(t *testing.T) {
		got, err := CalculateGostDigestFromImage("test:v1", img)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 32 {
			t.Errorf("expected 32-byte hash, got %d", len(got))
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		hash1, _ := CalculateGostDigestFromImage("test:v1", img)
		hash2, _ := CalculateGostDigestFromImage("test:v1", img)
		if hex.EncodeToString(hash1) != hex.EncodeToString(hash2) {
			t.Error("expected same hash for same image")
		}
	})
}

func TestAddGostDigestToImage(t *testing.T) {
	img, _ := random.Image(256, 2)

	t.Run("success", func(t *testing.T) {
		result, err := AddGostDigestToImage("test:v1", img)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result.DigestHex) != 64 { // hex-encoded 32 bytes
			t.Errorf("expected 64-char hex, got %d", len(result.DigestHex))
		}
		if result.Image == nil {
			t.Fatal("expected annotated image")
		}

		// Verify annotation was added
		manifest, _ := result.Image.Manifest()
		if manifest.Annotations[GostDigestAnnotationKey] != result.DigestHex {
			t.Errorf("annotation not set correctly")
		}
	})

	t.Run("digest matches recalculation", func(t *testing.T) {
		result, _ := AddGostDigestToImage("test:v1", img)

		// Recalculate and compare
		recalculated, _ := CalculateGostDigestFromImage("test:v1", img)
		if result.DigestHex != hex.EncodeToString(recalculated) {
			t.Error("digest doesn't match recalculation")
		}
	})
}

func TestValidateGostDigestFromImage(t *testing.T) {
	t.Run("no annotation error", func(t *testing.T) {
		img, _ := random.Image(256, 1)

		_, err := ValidateGostDigestFromImage("test:v1", img)
		if err == nil || !strings.Contains(err.Error(), "does not contain GOST digest") {
			t.Errorf("expected no annotation error, got %v", err)
		}
	})

	t.Run("success with valid annotation", func(t *testing.T) {
		img, _ := random.Image(256, 2)

		// Use AddGostDigestToImage to create properly annotated image
		addResult, err := AddGostDigestToImage("test:v1", img)
		if err != nil {
			t.Fatalf("failed to add digest: %v", err)
		}

		result, err := ValidateGostDigestFromImage("test:v1", addResult.Image)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.StoredDigest != addResult.DigestHex {
			t.Errorf("StoredDigest = %s, want %s", result.StoredDigest, addResult.DigestHex)
		}
		if result.CalculatedDigest != addResult.DigestHex {
			t.Errorf("CalculatedDigest = %s, want %s", result.CalculatedDigest, addResult.DigestHex)
		}
	})

	t.Run("mismatch with incorrect annotation", func(t *testing.T) {
		img, _ := random.Image(256, 2)

		// Create image with incorrect GOST annotation
		wrongDigest := hex.EncodeToString(make([]byte, 32)) // all zeros
		annotatedImg := mutate.Annotations(img, map[string]string{
			GostDigestAnnotationKey: wrongDigest,
		}).(v1.Image)

		result, err := ValidateGostDigestFromImage("test:v1", annotatedImg)
		if err == nil || !strings.Contains(err.Error(), "mismatch") {
			t.Errorf("expected mismatch error, got %v", err)
		}
		if result == nil {
			t.Fatal("expected result to be returned even on mismatch")
		}
		if result.StoredDigest != wrongDigest {
			t.Errorf("StoredDigest = %s, want %s", result.StoredDigest, wrongDigest)
		}
		if result.CalculatedDigest == wrongDigest {
			t.Error("CalculatedDigest should differ from wrong stored digest")
		}
	})
}
