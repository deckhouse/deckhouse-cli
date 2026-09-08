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

package image_test

import (
	"context"
	"os"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/types"

	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	dkpclient "github.com/deckhouse/deckhouse/pkg/registry/client"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/image"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

const resolveHost = "registry.example.com"

// resolveTestEnv holds an in-memory registry with a single-arch image and a
// multi-arch index under fixed tags, plus Options wired to reach it.
//
// The fake replaces the HTTP registry these tests used to stand up: Resolve is
// classification and cache-wrapping logic, so the wire adds seconds per test
// and, as the completion suite records, timeout flake under parallel
// `go test ./...` load. The layer that does need real HTTP is covered by
// cmd/integration_test.go.
type resolveTestEnv struct {
	opts     *registry.Options
	imageRef string // single-manifest image
	indexRef string // OCI index with linux/amd64 + linux/arm64
}

func setupResolveEnv(t *testing.T) *resolveTestEnv {
	t.Helper()

	reg := upfake.NewRegistry(resolveHost)
	reg.MustAddImage("app", "single", randomImage(t, 64))
	reg.MustAddIndex("app", "multi", multiArchIndex(t))

	opts := registry.New()
	opts.ClientFactory = func(string, ...dkpclient.Option) dkpreg.Client {
		return upfake.NewClient(reg)
	}

	return &resolveTestEnv{
		opts:     opts,
		imageRef: resolveHost + "/app:single",
		indexRef: resolveHost + "/app:multi",
	}
}

func randomImage(t *testing.T, size int64) v1.Image {
	t.Helper()

	img, err := random.Image(size, 1)
	if err != nil {
		t.Fatalf("random.Image: %v", err)
	}

	return img
}

// multiArchIndex builds an index whose children carry real platform
// descriptors, so --platform has something to match on.
func multiArchIndex(t *testing.T) v1.ImageIndex {
	t.Helper()

	idx := mutate.IndexMediaType(empty.Index, types.OCIImageIndex)

	for _, arch := range []string{"amd64", "arm64"} {
		img := randomImage(t, 32)

		cfg, err := img.ConfigFile()
		if err != nil {
			t.Fatalf("ConfigFile: %v", err)
		}

		cfg.OS, cfg.Architecture = "linux", arch

		img, err = mutate.ConfigFile(img, cfg)
		if err != nil {
			t.Fatalf("mutate.ConfigFile: %v", err)
		}

		idx = mutate.AppendManifests(idx, mutate.IndexAddendum{
			Add: img,
			Descriptor: v1.Descriptor{
				Platform: &v1.Platform{OS: "linux", Architecture: arch},
			},
		})
	}

	return idx
}

func mapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestResolve_SingleImage(t *testing.T) {
	env := setupResolveEnv(t)
	out, err := image.Resolve(context.Background(), []string{env.imageRef}, false, "", env.opts)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, ok := out.Images[env.imageRef]; !ok {
		t.Errorf("expected image in Images map, got Images=%v Indices=%v", mapKeys(out.Images), mapKeys(out.Indices))
	}
	if len(out.Indices) != 0 {
		t.Errorf("expected no Indices, got %v", mapKeys(out.Indices))
	}
}

func TestResolve_IndexKeptWhenNoPlatform(t *testing.T) {
	env := setupResolveEnv(t)
	out, err := image.Resolve(context.Background(), []string{env.indexRef}, true, "", env.opts)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, ok := out.Indices[env.indexRef]; !ok {
		t.Errorf("expected index in Indices map (keepMultiArchIndex=true, no platform), got Images=%v Indices=%v",
			mapKeys(out.Images), mapKeys(out.Indices))
	}
}

func TestResolve_IndexFlattenedWhenPlatformPinned(t *testing.T) {
	env := setupResolveEnv(t)
	opts := env.opts.WithPlatform(&v1.Platform{OS: "linux", Architecture: "amd64"})
	out, err := image.Resolve(context.Background(), []string{env.indexRef}, true, "", opts)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, ok := out.Images[env.indexRef]; !ok {
		t.Errorf("expected platform-pinned index to resolve to single Image, got Images=%v Indices=%v",
			mapKeys(out.Images), mapKeys(out.Indices))
	}
	if len(out.Indices) != 0 {
		t.Errorf("expected no Indices when platform pinned, got %v", mapKeys(out.Indices))
	}
}

func TestResolve_IndexFlattenedWhenKeepFalse(t *testing.T) {
	env := setupResolveEnv(t)
	out, err := image.Resolve(context.Background(), []string{env.indexRef}, false, "", env.opts)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, ok := out.Images[env.indexRef]; !ok {
		t.Errorf("expected index to flatten to Image when keepMultiArchIndex=false, got Images=%v Indices=%v",
			mapKeys(out.Images), mapKeys(out.Indices))
	}
}

func TestResolve_CachePathWrapsImage(t *testing.T) {
	env := setupResolveEnv(t)
	cacheDir := t.TempDir()
	out, err := image.Resolve(context.Background(), []string{env.imageRef}, false, cacheDir, env.opts)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	img, ok := out.Images[env.imageRef]
	if !ok {
		t.Fatalf("missing image in Images map")
	}
	// Materializing layers populates the cache directory.
	layers, err := img.Layers()
	if err != nil {
		t.Fatalf("Layers: %v", err)
	}
	for _, l := range layers {
		rc, err := l.Compressed()
		if err != nil {
			t.Fatalf("Compressed: %v", err)
		}
		buf := make([]byte, 4096)
		for {
			if _, err := rc.Read(buf); err != nil {
				break
			}
		}
		_ = rc.Close()
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("ReadDir cache: %v", err)
	}
	if len(entries) == 0 {
		t.Errorf("expected cache directory to be populated, got 0 entries in %s", cacheDir)
	}
}

// Pre-fix: --cache-path was silently dropped on the index path, so
// `pull --format oci` of a multi-arch image (the common shape) re-downloaded
// every layer on every run. Resolve must wrap the index with cache.ImageIndex
// symmetrically with the single-image branch.
func TestResolve_CachePathWrapsIndex(t *testing.T) {
	env := setupResolveEnv(t)
	cacheDir := t.TempDir()
	out, err := image.Resolve(context.Background(), []string{env.indexRef}, true, cacheDir, env.opts)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	idx, ok := out.Indices[env.indexRef]
	if !ok {
		t.Fatalf("missing index in Indices map")
	}
	manifest, err := idx.IndexManifest()
	if err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	for _, desc := range manifest.Manifests {
		child, err := idx.Image(desc.Digest)
		if err != nil {
			t.Fatalf("Image(%s): %v", desc.Digest, err)
		}
		layers, err := child.Layers()
		if err != nil {
			t.Fatalf("child Layers: %v", err)
		}
		for _, l := range layers {
			rc, err := l.Compressed()
			if err != nil {
				t.Fatalf("Compressed: %v", err)
			}
			buf := make([]byte, 4096)
			for {
				if _, err := rc.Read(buf); err != nil {
					break
				}
			}
			_ = rc.Close()
		}
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("ReadDir cache: %v", err)
	}
	if len(entries) == 0 {
		t.Errorf("expected cache directory to be populated by index pull, got 0 entries in %s", cacheDir)
	}
}

func TestResolve_MultipleSources(t *testing.T) {
	env := setupResolveEnv(t)
	out, err := image.Resolve(context.Background(),
		[]string{env.imageRef, env.indexRef},
		true, "", env.opts,
	)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, ok := out.Images[env.imageRef]; !ok {
		t.Errorf("missing single image in Images")
	}
	if _, ok := out.Indices[env.indexRef]; !ok {
		t.Errorf("missing index in Indices")
	}
}

// Duplicate refs would silently collapse into a single map slot, dropping
// copies that the caller asked for - `cr pull foo:1 foo:1 dst.tar` would
// produce a single-image tarball. Make that an explicit error instead.
func TestResolve_DuplicateRefsAreRejected(t *testing.T) {
	env := setupResolveEnv(t)
	_, err := image.Resolve(context.Background(),
		[]string{env.imageRef, env.imageRef},
		false, "", env.opts,
	)
	if err == nil {
		t.Fatalf("expected duplicate-ref error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate source reference") {
		t.Fatalf("expected duplicate-ref error, got: %v", err)
	}
}

// nil opts is a programmer error - explicit early failure beats a panic
// inside FetchDescriptor's name.ParseReference.
func TestResolve_NilOptsReturnsError(t *testing.T) {
	_, err := image.Resolve(context.Background(), []string{"alpine:3.19"}, false, "", nil)
	if err == nil {
		t.Fatalf("expected error on nil opts, got nil")
	}
}
