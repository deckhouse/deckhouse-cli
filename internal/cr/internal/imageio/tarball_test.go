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

package imageio

import (
	"path/filepath"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/random"
)

func randomImage(t *testing.T) v1.Image {
	t.Helper()
	img, err := random.Image(128, 1)
	if err != nil {
		t.Fatalf("random.Image: %v", err)
	}
	return img
}

func TestSaveTarball_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "image.tar")
	src := randomImage(t)

	err := SaveTarball(path, map[string]v1.Image{"example.com/test:v1": src})
	if err != nil {
		t.Fatalf("SaveTarball: %v", err)
	}

	loaded, err := LoadTarball(path, "")
	if err != nil {
		t.Fatalf("LoadTarball: %v", err)
	}

	gotDigest, err := loaded.Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	wantDigest, err := src.Digest()
	if err != nil {
		t.Fatalf("src digest: %v", err)
	}
	if gotDigest != wantDigest {
		t.Errorf("digest mismatch: got %s, want %s", gotDigest, wantDigest)
	}
}

func TestSaveLegacy_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.tar")
	src := randomImage(t)

	// Legacy tarballs require tagged references; digest refs would be rejected.
	err := SaveLegacy(path, map[string]v1.Image{"example.com/test:legacy": src})
	if err != nil {
		t.Fatalf("SaveLegacy: %v", err)
	}

	loaded, err := LoadTarball(path, "example.com/test:legacy")
	if err != nil {
		t.Fatalf("LoadTarball: %v", err)
	}
	if _, err := loaded.Digest(); err != nil {
		t.Errorf("digest of reloaded legacy image: %v", err)
	}
}

func TestSaveLegacy_RejectsDigestRef(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.tar")
	src := randomImage(t)
	err := SaveLegacy(path, map[string]v1.Image{
		"example.com/test@sha256:0000000000000000000000000000000000000000000000000000000000000000": src,
	})
	if err == nil {
		t.Fatalf("SaveLegacy should reject digest references")
	}
}

// SaveTarball used to silently emit a tar with no manifest.json on an empty
// map, which LoadTarball then could not read back. Reject up front so callers
// see a clean failure.
func TestSaveTarball_RejectsEmptyMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.tar")
	if err := SaveTarball(path, map[string]v1.Image{}); err == nil {
		t.Fatalf("SaveTarball should reject empty input")
	}
}

func TestSaveLegacy_RejectsEmptyMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty-legacy.tar")
	if err := SaveLegacy(path, map[string]v1.Image{}); err == nil {
		t.Fatalf("SaveLegacy should reject empty input")
	}
}

func TestLoadTarball_Missing(t *testing.T) {
	if _, err := LoadTarball(filepath.Join(t.TempDir(), "none.tar"), ""); err == nil {
		t.Fatalf("LoadTarball should error on missing file")
	}
}
