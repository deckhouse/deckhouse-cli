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

// Tests for the adapter between `d8 cr`'s reference-oriented commands and the
// repository-scoped deckhouse/pkg/registry client, driven against that
// package's in-memory fake.
//
// The fake keeps these off the network: an HTTP registry per test costs seconds
// and, as the completion suite records, produces timeout flake under parallel
// `go test ./...` load. What the fake cannot stand in for stays on a real
// registry - see tags_test.go for the Link-cursor walk and login_test.go for
// the auth handshakes, neither of which the fake models.
package registry_test

import (
	"context"
	"errors"
	"slices"
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

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

const (
	fakeHost  = "registry.example.com"
	otherHost = "mirror.example.com"
)

// fakeEnv wires Options to a set of in-memory registries keyed by host.
type fakeEnv struct {
	opts       *registry.Options
	registries map[string]*upfake.Registry
}

// newFakeEnv seeds fakeHost with app:v1 (plus a second tag) and returns Options
// whose ClientFactory routes by host.
//
// Routing matters: the fake resolves a bare path against the first registry it
// was given, so the factory has to hand back a client whose default host is the
// one the reference actually named - otherwise a two-registry test (push from
// one to another) would silently talk to the wrong one.
func newFakeEnv(t *testing.T) *fakeEnv {
	t.Helper()

	primary := upfake.NewRegistry(fakeHost)
	primary.MustAddImage("app", "v1", upfake.NewImageBuilder().
		WithFile("etc/version", "1.0\n").
		WithLabel("owner", "platform").
		MustBuild())
	primary.MustAddImage("app", "v2", upfake.NewImageBuilder().MustBuild())
	primary.MustAddImage("tools/scanner", "latest", upfake.NewImageBuilder().MustBuild())

	env := &fakeEnv{
		registries: map[string]*upfake.Registry{
			fakeHost:  primary,
			otherHost: upfake.NewRegistry(otherHost),
		},
	}

	env.opts = registry.New()
	env.opts.ClientFactory = func(host string, _ ...dkpclient.Option) dkpreg.Client {
		reg, ok := env.registries[host]
		if !ok {
			// An unknown host must fail the way a real one does rather than
			// falling back to some other registry in the map.
			return upfake.NewClient(upfake.NewRegistry(host))
		}

		return upfake.NewClient(reg)
	}

	return env
}

func (e *fakeEnv) ref(repoTag string) string { return fakeHost + "/" + repoTag }

func TestFetch_ResolvesReferenceToImage(t *testing.T) {
	env := newFakeEnv(t)

	img, err := registry.Fetch(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		t.Fatalf("ConfigFile: %v", err)
	}

	if got := cfg.Config.Labels["owner"]; got != "platform" {
		t.Errorf("resolved the wrong image: label owner = %q, want platform", got)
	}
}

// A repository path with several segments has to survive the split into client
// segments; joining it wrong addresses "host/tools" or "host/tools%2Fscanner".
func TestFetch_HandlesNestedRepositoryPath(t *testing.T) {
	env := newFakeEnv(t)

	if _, err := registry.Fetch(context.Background(), env.ref("tools/scanner:latest"), env.opts); err != nil {
		t.Fatalf("Fetch on a nested repository path: %v", err)
	}
}

func TestFetch_MissingTagReportsNotFound(t *testing.T) {
	env := newFakeEnv(t)

	_, err := registry.Fetch(context.Background(), env.ref("app:nope"), env.opts)
	if err == nil {
		t.Fatalf("expected an error for a missing tag")
	}

	// The sentinel has to survive the adapter's wrapping, or every caller is
	// back to matching on message text.
	if !errors.Is(err, dkpreg.ErrImageNotFound) {
		t.Errorf("error should unwrap to ErrImageNotFound; got %v", err)
	}

	if !strings.Contains(err.Error(), env.ref("app:nope")) {
		t.Errorf("error should name the reference the user typed; got %v", err)
	}
}

func TestFetch_UnknownHostFails(t *testing.T) {
	env := newFakeEnv(t)

	if _, err := registry.Fetch(context.Background(), "nowhere.example.com/app:v1", env.opts); err == nil {
		t.Fatalf("expected an error for a host with no such image")
	}
}

func TestFetch_RejectsUnparseableReference(t *testing.T) {
	env := newFakeEnv(t)

	_, err := registry.Fetch(context.Background(), "NOT A REFERENCE", env.opts)
	if err == nil {
		t.Fatalf("expected a parse error")
	}

	if !strings.Contains(err.Error(), "parse reference") {
		t.Errorf("error should say the reference failed to parse; got %v", err)
	}
}

// `cr manifest` exists to hand exact bytes to a signature verifier, so the
// adapter must not round-trip the manifest through a decoder.
func TestFetchManifest_ReturnsBytesAsServed(t *testing.T) {
	env := newFakeEnv(t)

	got, err := registry.FetchManifest(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("FetchManifest: %v", err)
	}

	img, err := registry.Fetch(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	want, err := img.RawManifest()
	if err != nil {
		t.Fatalf("RawManifest: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("manifest bytes differ from the stored manifest\n got: %s\nwant: %s", got, want)
	}
}

func TestFetchConfig_ReturnsRawJSON(t *testing.T) {
	env := newFakeEnv(t)

	got, err := registry.FetchConfig(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("FetchConfig: %v", err)
	}

	if !strings.Contains(string(got), `"owner":"platform"`) {
		t.Errorf("config JSON should carry the image labels verbatim; got %s", got)
	}
}

// The digest of a manifest is the hash of its own bytes, so the adapter's
// answer has to match what the stored image reports.
func TestFetchDigest_MatchesTheImageDigest(t *testing.T) {
	env := newFakeEnv(t)

	got, err := registry.FetchDigest(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("FetchDigest: %v", err)
	}

	img, err := registry.Fetch(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	want, err := img.Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}

	if got != want.String() {
		t.Errorf("digest = %s, want %s", got, want)
	}
}

// A digest reference must address the same manifest a tag does; the client
// builds "host/repo@sha256:..." from the identifier, and getting that wrong
// silently turns the digest into a tag named "sha256:...".
func TestFetch_AcceptsDigestReference(t *testing.T) {
	env := newFakeEnv(t)

	digest, err := registry.FetchDigest(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("FetchDigest: %v", err)
	}

	if _, err := registry.Fetch(context.Background(), env.ref("app@"+digest), env.opts); err != nil {
		t.Errorf("Fetch by digest: %v", err)
	}
}

func TestIsIndex_SingleImageIsNotAnIndex(t *testing.T) {
	env := newFakeEnv(t)

	isIndex, err := registry.IsIndex(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("IsIndex: %v", err)
	}

	if isIndex {
		t.Errorf("a single-manifest image must not be classified as an index")
	}
}

func TestListTags_ReturnsEveryTag(t *testing.T) {
	env := newFakeEnv(t)

	tags, err := registry.ListTags(context.Background(), fakeHost+"/app", env.opts)
	if err != nil {
		t.Fatalf("ListTags: %v", err)
	}

	if want := []string{"v1", "v2"}; !slices.Equal(tags, want) {
		t.Errorf("tags = %v, want %v", tags, want)
	}
}

func TestListCatalog_ReturnsEveryRepository(t *testing.T) {
	env := newFakeEnv(t)

	repos, err := registry.ListCatalog(context.Background(), fakeHost, env.opts)
	if err != nil {
		t.Fatalf("ListCatalog: %v", err)
	}

	if !slices.Contains(repos, "app") || !slices.Contains(repos, "tools/scanner") {
		t.Errorf("catalog should list every repository; got %v", repos)
	}
}

func TestPush_WritesImageAndReturnsItsDigest(t *testing.T) {
	env := newFakeEnv(t)

	img, err := registry.Fetch(context.Background(), env.ref("app:v1"), env.opts)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	dst := otherHost + "/copied:v1"

	digest, err := registry.Push(context.Background(), dst, img, env.opts)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	want, err := img.Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}

	if digest != want {
		t.Errorf("Push returned %s, want the pushed image's digest %s", digest, want)
	}

	// Pushing to the second host must land there, not back in the source.
	if _, err := registry.Fetch(context.Background(), dst, env.opts); err != nil {
		t.Errorf("pushed image is not readable back from %s: %v", otherHost, err)
	}
}

func TestPush_RejectsNilObject(t *testing.T) {
	env := newFakeEnv(t)

	if _, err := registry.Push(context.Background(), env.ref("app:v1"), nil, env.opts); err == nil {
		t.Fatalf("expected an error for a nil object")
	}
}

// A typed-nil v1.Image arrives as a non-nil interface holding a nil pointer,
// which the plain nil check above does not catch.
func TestPush_RejectsUnsupportedType(t *testing.T) {
	env := newFakeEnv(t)

	if _, err := registry.Push(context.Background(), env.ref("app:v1"), notAnImage{}, env.opts); err == nil {
		t.Fatalf("expected an error for an object that is neither an image nor an index")
	}
}

type notAnImage struct{}

func (notAnImage) RawManifest() ([]byte, error) { return nil, nil }

func (notAnImage) MediaType() (types.MediaType, error) { return "", nil }

func (notAnImage) Digest() (v1.Hash, error) { return v1.Hash{}, nil }

func (notAnImage) Size() (int64, error) { return 0, nil }

// ---- multi-arch, now that the fake models indexes ----

// withIndex adds a two-platform index under app:multi.
func (e *fakeEnv) withIndex(t *testing.T) string {
	t.Helper()

	idx := mutate.IndexMediaType(empty.Index, types.OCIImageIndex)

	for _, arch := range []string{"amd64", "arm64"} {
		idx = mutate.AppendManifests(idx, mutate.IndexAddendum{
			Add:        platformImage(t, arch),
			Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: arch}},
		})
	}

	e.registries[fakeHost].MustAddIndex("app", "multi", idx)

	return e.ref("app:multi")
}

func TestIsIndex_MultiArchReferenceIsAnIndex(t *testing.T) {
	env := newFakeEnv(t)
	ref := env.withIndex(t)

	isIndex, err := registry.IsIndex(context.Background(), ref, env.opts)
	if err != nil {
		t.Fatalf("IsIndex: %v", err)
	}

	if !isIndex {
		t.Errorf("a multi-arch reference must be classified as an index")
	}
}

func TestFetchIndex_ReturnsEveryPlatform(t *testing.T) {
	env := newFakeEnv(t)
	ref := env.withIndex(t)

	idx, err := registry.FetchIndex(context.Background(), ref, env.opts)
	if err != nil {
		t.Fatalf("FetchIndex: %v", err)
	}

	manifest, err := idx.IndexManifest()
	if err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	if got := len(manifest.Manifests); got != 2 {
		t.Errorf("index resolved to %d manifests, want both platforms", got)
	}
}

// FetchIndex on a plain image must fail rather than wrap it in a synthetic
// one-entry index, which pull would then write to disk as a bogus layout.
func TestFetchIndex_RejectsSingleImage(t *testing.T) {
	env := newFakeEnv(t)

	if _, err := registry.FetchIndex(context.Background(), env.ref("app:v1"), env.opts); err == nil {
		t.Fatalf("expected an error for a single-image reference")
	}
}

// The digest of a multi-arch reference is the index digest; with a platform it
// has to be that child's, otherwise pinning a deployment pins the wrong thing.
func TestFetchDigest_PlatformResolvesToChild(t *testing.T) {
	env := newFakeEnv(t)
	ref := env.withIndex(t)

	indexDigest, err := registry.FetchDigest(context.Background(), ref, env.opts)
	if err != nil {
		t.Fatalf("FetchDigest: %v", err)
	}

	env.opts.WithPlatform(&v1.Platform{OS: "linux", Architecture: "arm64"})

	childDigest, err := registry.FetchDigest(context.Background(), ref, env.opts)
	if err != nil {
		t.Fatalf("FetchDigest with a platform: %v", err)
	}

	if childDigest == indexDigest {
		t.Errorf("--platform was ignored: still the index digest %s", indexDigest)
	}

	// The child digest must be one the index actually lists.
	full, err := registry.FetchIndex(context.Background(), ref, env.opts)
	if err != nil {
		t.Fatalf("FetchIndex: %v", err)
	}

	manifest, err := full.IndexManifest()
	if err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	var found bool

	for _, child := range manifest.Manifests {
		if child.Digest.String() == childDigest {
			found = true

			if child.Platform.Architecture != "arm64" {
				t.Errorf("resolved to %s, want arm64", child.Platform.Architecture)
			}
		}
	}

	if !found {
		t.Errorf("digest %s is not a child of the index", childDigest)
	}
}

// Without a platform the underlying library resolves an index to a hardcoded
// linux/amd64 rather than the host's platform. That is a trap worth pinning
// down: a caller on arm64 who omits --platform silently gets amd64.
func TestFetch_IndexWithoutPlatformDefaultsToAmd64(t *testing.T) {
	env := newFakeEnv(t)
	ref := env.withIndex(t)

	img, err := registry.Fetch(context.Background(), ref, env.opts)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		t.Fatalf("ConfigFile: %v", err)
	}

	if cfg.Architecture != "amd64" {
		t.Errorf("architecture = %q, want amd64 (the library's hardcoded default)", cfg.Architecture)
	}
}

// platformImage builds a child whose config names the architecture, not just
// its index descriptor: resolving a platform hands back the image, so a test
// that reads the architecture back has to find it in the config.
func platformImage(t *testing.T, arch string) v1.Image {
	t.Helper()

	img, err := random.Image(32, 1)
	if err != nil {
		t.Fatalf("random.Image: %v", err)
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		t.Fatalf("ConfigFile: %v", err)
	}

	cfg.OS, cfg.Architecture = "linux", arch

	img, err = mutate.ConfigFile(img, cfg)
	if err != nil {
		t.Fatalf("mutate.ConfigFile: %v", err)
	}

	return img
}
