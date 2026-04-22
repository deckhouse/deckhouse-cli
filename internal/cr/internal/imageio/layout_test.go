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
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
)

func TestSaveOCI_WritesImage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "layout")
	img := randomImage(t)

	err := SaveOCI(dir, map[string]v1.Image{"example.com/app:v1": img}, nil)
	if err != nil {
		t.Fatalf("SaveOCI: %v", err)
	}

	// Verify we wrote a layout with one manifest.
	idx, err := layout.ImageIndexFromPath(dir)
	if err != nil {
		t.Fatalf("layout.ImageIndexFromPath: %v", err)
	}
	manifest, err := idx.IndexManifest()
	if err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	if len(manifest.Manifests) != 1 {
		t.Fatalf("expected 1 manifest in layout, got %d", len(manifest.Manifests))
	}
}

func TestSaveOCI_RejectsRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "image.tar")
	if err := os.WriteFile(path, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err := SaveOCI(path, map[string]v1.Image{"example.com/app:v1": randomImage(t)}, nil)
	if err == nil {
		t.Fatalf("expected error for regular-file destination")
	}
	if !strings.Contains(err.Error(), "requires a directory") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// SaveOCI must refuse to clobber a non-empty directory that does not look
// like an OCI layout. Pre-fix behavior: layout.FromPath errored, layout.Write
// then planted oci-layout / index.json into the user's existing directory
// alongside their files. The classic footgun is a typo on the destination
// path (e.g. `cr pull --format oci alpine ~/Documents`).
func TestSaveOCI_RejectsNonEmptyNonLayoutDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "userdata")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	stranger := filepath.Join(dir, "important.txt")
	if err := os.WriteFile(stranger, []byte("user data"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	err := SaveOCI(dir, map[string]v1.Image{"example.com/app:v1": randomImage(t)}, nil)
	if err == nil {
		t.Fatalf("expected refusal, got nil")
	}
	if !strings.Contains(err.Error(), "not an OCI layout") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Pre-existing user file must still be intact, no layout files added.
	got, err := os.ReadFile(stranger)
	if err != nil || string(got) != "user data" {
		t.Fatalf("user file corrupted: content=%q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "oci-layout")); !os.IsNotExist(err) {
		t.Errorf("oci-layout should not have been created, Stat err=%v", err)
	}
}

// An existing empty directory is a legitimate destination - the user might
// have created it ahead of time. Must not error.
func TestSaveOCI_EmptyExistingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fresh")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := SaveOCI(dir, map[string]v1.Image{"example.com/app:v1": randomImage(t)}, nil); err != nil {
		t.Fatalf("SaveOCI on empty dir: %v", err)
	}
}

// Re-running SaveOCI against an already-valid layout must keep appending
// without surfacing the "non-empty" guard erroneously.
func TestSaveOCI_AppendsToExistingLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "layout")
	if err := SaveOCI(dir, map[string]v1.Image{"example.com/app:v1": randomImage(t)}, nil); err != nil {
		t.Fatalf("first SaveOCI: %v", err)
	}
	if err := SaveOCI(dir, map[string]v1.Image{"example.com/app:v2": randomImage(t)}, nil); err != nil {
		t.Fatalf("second SaveOCI: %v", err)
	}
	idx, err := layout.ImageIndexFromPath(dir)
	if err != nil {
		t.Fatalf("read layout: %v", err)
	}
	manifest, err := idx.IndexManifest()
	if err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	if len(manifest.Manifests) != 2 {
		t.Errorf("expected 2 manifests after append, got %d", len(manifest.Manifests))
	}
}

func makeLayoutWithImages(t *testing.T, n int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "layout")
	p, err := layout.Write(dir, empty.Index)
	if err != nil {
		t.Fatalf("layout.Write: %v", err)
	}
	for range n {
		img, err := random.Image(64, 1)
		if err != nil {
			t.Fatalf("random.Image: %v", err)
		}
		if err := p.AppendImage(img); err != nil {
			t.Fatalf("AppendImage: %v", err)
		}
	}
	return dir
}

func TestLoadLocal_SingleImageLayout(t *testing.T) {
	dir := makeLayoutWithImages(t, 1)
	obj, err := LoadLocal(dir, false)
	if err != nil {
		t.Fatalf("LoadLocal: %v", err)
	}
	if _, ok := obj.(v1.Image); !ok {
		t.Errorf("single-image layout should yield v1.Image, got %T", obj)
	}
}

func TestLoadLocal_NestedIndexLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "layout")
	p, err := layout.Write(dir, empty.Index)
	if err != nil {
		t.Fatalf("layout.Write: %v", err)
	}
	inner, err := random.Index(32, 1, 2)
	if err != nil {
		t.Fatalf("random.Index: %v", err)
	}
	if err := p.AppendIndex(inner); err != nil {
		t.Fatalf("AppendIndex: %v", err)
	}

	obj, err := LoadLocal(dir, false)
	if err != nil {
		t.Fatalf("LoadLocal: %v", err)
	}
	if _, ok := obj.(v1.ImageIndex); !ok {
		t.Errorf("layout with single inner index should yield v1.ImageIndex, got %T", obj)
	}
}

func TestLoadLocal_MultipleManifestsRequiresIndex(t *testing.T) {
	dir := makeLayoutWithImages(t, 3)
	if _, err := LoadLocal(dir, false); err == nil {
		t.Fatalf("expected error for multi-manifest layout without --index")
	}
	obj, err := LoadLocal(dir, true)
	if err != nil {
		t.Fatalf("LoadLocal with asIndex=true: %v", err)
	}
	if _, ok := obj.(v1.ImageIndex); !ok {
		t.Errorf("asIndex=true should yield v1.ImageIndex, got %T", obj)
	}
}

func TestLoadLocal_TarballFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "image.tar")
	src := randomImage(t)
	if err := SaveTarball(path, map[string]v1.Image{"example.com/app:v1": src}); err != nil {
		t.Fatalf("SaveTarball: %v", err)
	}
	obj, err := LoadLocal(path, false)
	if err != nil {
		t.Fatalf("LoadLocal tarball: %v", err)
	}
	if _, ok := obj.(v1.Image); !ok {
		t.Errorf("tarball should yield v1.Image, got %T", obj)
	}
}

func TestLoadLocal_Missing(t *testing.T) {
	if _, err := LoadLocal(filepath.Join(t.TempDir(), "does-not-exist"), false); err == nil {
		t.Fatalf("expected error for missing path")
	}
}
