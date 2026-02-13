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
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/random"
)

// =============================================================================
// Layer 1: Pure Hash Computation Tests
// =============================================================================

func TestCalculateGostHash(t *testing.T) {
	t.Run("returns 32 bytes", func(t *testing.T) {
		hash := CalculateGostHash([]byte("test data"))
		if len(hash) != 32 {
			t.Errorf("expected 32-byte hash, got %d", len(hash))
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		data := []byte("test data")
		hash1 := CalculateGostHash(data)
		hash2 := CalculateGostHash(data)
		if !bytes.Equal(hash1, hash2) {
			t.Error("expected same hash for same input")
		}
	})

	t.Run("different input different output", func(t *testing.T) {
		hash1 := CalculateGostHash([]byte("data1"))
		hash2 := CalculateGostHash([]byte("data2"))
		if bytes.Equal(hash1, hash2) {
			t.Error("expected different hashes for different inputs")
		}
	})
}

func TestCalculateGostHashFromReader(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		reader := strings.NewReader("test data")
		hash, err := CalculateGostHashFromReader(reader)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(hash) != 32 {
			t.Errorf("expected 32-byte hash, got %d", len(hash))
		}
	})

	t.Run("matches direct calculation", func(t *testing.T) {
		data := "test data"
		directHash := CalculateGostHash([]byte(data))
		readerHash, _ := CalculateGostHashFromReader(strings.NewReader(data))
		if !bytes.Equal(directHash, readerHash) {
			t.Error("reader hash should match direct hash")
		}
	})
}

// =============================================================================
// Layer 2: Layer Digest Extraction Tests
// =============================================================================

func TestExtractSortedLayerDigests(t *testing.T) {
	img, err := random.Image(256, 3)
	if err != nil {
		t.Fatalf("failed to create test image: %v", err)
	}

	t.Run("extracts correct count", func(t *testing.T) {
		digests, err := ExtractSortedLayerDigests(img)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(digests) != 3 {
			t.Errorf("expected 3 digests, got %d", len(digests))
		}
	})

	t.Run("digests are sorted", func(t *testing.T) {
		digests, _ := ExtractSortedLayerDigests(img)
		for i := 1; i < len(digests); i++ {
			if digests[i-1] > digests[i] {
				t.Error("digests not sorted")
			}
		}
	})
}

func TestReadGostAnnotation(t *testing.T) {
	img, _ := random.Image(256, 1)

	t.Run("no annotation", func(t *testing.T) {
		digest, ok, err := ReadGostAnnotation(img)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Error("expected ok=false for image without annotation")
		}
		if digest != "" {
			t.Error("expected empty digest")
		}
	})

	t.Run("with annotation", func(t *testing.T) {
		annotated := AddGostAnnotation(img, "abc123")
		digest, ok, err := ReadGostAnnotation(annotated)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Error("expected ok=true for annotated image")
		}
		if digest != "abc123" {
			t.Errorf("expected 'abc123', got '%s'", digest)
		}
	})
}

func TestAddGostAnnotation(t *testing.T) {
	img, _ := random.Image(256, 1)

	t.Run("adds annotation", func(t *testing.T) {
		annotated := AddGostAnnotation(img, "test-digest")
		manifest, _ := annotated.Manifest()
		if manifest.Annotations[GostDigestAnnotationKey] != "test-digest" {
			t.Error("annotation not set correctly")
		}
	})
}

// =============================================================================
// Layer 3: Composed Operations Tests
// =============================================================================

func TestCalculateImageGostDigest(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		img, _ := random.Image(256, 2)
		digest, err := CalculateImageGostDigest(img)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(digest) != 32 {
			t.Errorf("expected 32-byte hash, got %d", len(digest))
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		img, _ := random.Image(256, 2)
		hash1, _ := CalculateImageGostDigest(img)
		hash2, _ := CalculateImageGostDigest(img)
		if !bytes.Equal(hash1, hash2) {
			t.Error("expected same hash for same image")
		}
	})
}

func TestAnnotateWithGostDigest(t *testing.T) {
	img, _ := random.Image(256, 2)

	t.Run("success", func(t *testing.T) {
		annotated, digestHex, err := AnnotateWithGostDigest(img)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(digestHex) != 64 {
			t.Errorf("expected 64-char hex, got %d", len(digestHex))
		}
		if annotated == nil {
			t.Fatal("expected annotated image")
		}

		// Verify annotation was added
		manifest, _ := annotated.Manifest()
		if manifest.Annotations[GostDigestAnnotationKey] != digestHex {
			t.Error("annotation not set correctly")
		}
	})

	t.Run("digest matches recalculation", func(t *testing.T) {
		_, digestHex, _ := AnnotateWithGostDigest(img)
		recalculated, _ := CalculateImageGostDigest(img)
		if digestHex != hex.EncodeToString(recalculated) {
			t.Error("digest doesn't match recalculation")
		}
	})
}

func TestValidateGostDigest(t *testing.T) {
	t.Run("no annotation error", func(t *testing.T) {
		img, _ := random.Image(256, 1)
		_, err := ValidateGostDigest(img)
		if err == nil || !strings.Contains(err.Error(), "does not contain GOST digest") {
			t.Errorf("expected no annotation error, got %v", err)
		}
	})

	t.Run("success with valid annotation", func(t *testing.T) {
		img, _ := random.Image(256, 2)
		annotated, expectedDigest, _ := AnnotateWithGostDigest(img)

		result, err := ValidateGostDigest(annotated)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.StoredDigest != expectedDigest {
			t.Errorf("StoredDigest = %s, want %s", result.StoredDigest, expectedDigest)
		}
		if result.CalculatedDigest != expectedDigest {
			t.Errorf("CalculatedDigest = %s, want %s", result.CalculatedDigest, expectedDigest)
		}
	})

	t.Run("mismatch with incorrect annotation", func(t *testing.T) {
		img, _ := random.Image(256, 2)
		wrongDigest := hex.EncodeToString(make([]byte, 32))
		annotated := AddGostAnnotation(img, wrongDigest)

		result, err := ValidateGostDigest(annotated)
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

// =============================================================================
// Utility: ImageInfo Tests
// =============================================================================

func TestGetImageInfo(t *testing.T) {
	img, _ := random.Image(256, 2)

	t.Run("extracts all fields", func(t *testing.T) {
		info, err := GetImageInfo("test:v1", img)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info.Name != "test:v1" {
			t.Errorf("Name = %s, want test:v1", info.Name)
		}
		if info.Digest == "" {
			t.Error("Digest is empty")
		}
		if len(info.LayerDigests) != 2 {
			t.Errorf("LayerDigests length = %d, want 2", len(info.LayerDigests))
		}
	})

	t.Run("includes GOST digest if present", func(t *testing.T) {
		annotated, expectedDigest, _ := AnnotateWithGostDigest(img)
		info, _ := GetImageInfo("test:v1", annotated)
		if info.GostDigest != expectedDigest {
			t.Errorf("GostDigest = %s, want %s", info.GostDigest, expectedDigest)
		}
	})
}
