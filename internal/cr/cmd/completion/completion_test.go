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

package completion

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/cobra"

	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

// ---------- parseRef ----------

func TestParseRef(t *testing.T) {
	cases := []struct {
		in   string
		want refParts
	}{
		{"", refParts{kind: kindEmpty}},
		{"docker.io", refParts{kind: kindHost, host: "docker.io"}},
		{"docker.io/", refParts{kind: kindHostSlash, host: "docker.io"}},
		{"docker.io/library", refParts{kind: kindRepoPath, host: "docker.io", repoPath: "library"}},
		{"docker.io/library/nginx", refParts{kind: kindRepoPath, host: "docker.io", repoPath: "library/nginx"}},
		{"docker.io/library/nginx:", refParts{kind: kindRepoColon, host: "docker.io", repoPath: "library/nginx"}},
		{"docker.io/library/nginx:latest", refParts{kind: kindRepoColon, host: "docker.io", repoPath: "library/nginx", tagPart: "latest"}},
		// Port in hostname must not be confused with tag separator.
		{"localhost:5000", refParts{kind: kindHost, host: "localhost:5000"}},
		{"localhost:5000/", refParts{kind: kindHostSlash, host: "localhost:5000"}},
		{"localhost:5000/repo", refParts{kind: kindRepoPath, host: "localhost:5000", repoPath: "repo"}},
		{"localhost:5000/repo:tag", refParts{kind: kindRepoColon, host: "localhost:5000", repoPath: "repo", tagPart: "tag"}},
		// Digest refs ('@sha256:...') must not be misclassified as tag-typing:
		// otherwise "repo@sha256" becomes the synthetic repo path and ListTags
		// goes hunting for it.
		{"docker.io/library/nginx@", refParts{kind: kindRepoDigest, host: "docker.io", repoPath: "library/nginx"}},
		{"docker.io/library/nginx@sha256:", refParts{kind: kindRepoDigest, host: "docker.io", repoPath: "library/nginx"}},
		{"docker.io/library/nginx@sha256:abcdef", refParts{kind: kindRepoDigest, host: "docker.io", repoPath: "library/nginx"}},
		{"localhost:5000/repo@sha256:abc", refParts{kind: kindRepoDigest, host: "localhost:5000", repoPath: "repo"}},
		// Trailing slash is noise - parseRef must collapse it so downstream
		// suggestion building cannot end up with "host//repo".
		{"docker.io/library/", refParts{kind: kindRepoPath, host: "docker.io", repoPath: "library"}},
		{"docker.io/library/nginx/", refParts{kind: kindRepoPath, host: "docker.io", repoPath: "library/nginx"}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := parseRef(tc.in)
			if got != tc.want {
				t.Fatalf("parseRef(%q):\n got  %+v\n want %+v", tc.in, got, tc.want)
			}
		})
	}
}

// ---------- filterByPrefix / normalizeHost ----------

func TestFilterByPrefix(t *testing.T) {
	in := []string{"alpha", "alpaca", "beta"}
	if got := filterByPrefix(in, ""); !slices.Equal(got, in) {
		t.Fatalf("empty prefix: got %v want %v", got, in)
	}
	if got := filterByPrefix(in, "alp"); !slices.Equal(got, []string{"alpha", "alpaca"}) {
		t.Fatalf("alp prefix: got %v", got)
	}
	if got := filterByPrefix(in, "z"); len(got) != 0 {
		t.Fatalf("z prefix: got %v want empty", got)
	}
}

func TestNormalizeHost(t *testing.T) {
	cases := map[string]string{
		"https://index.docker.io/v1/": "index.docker.io",
		"http://localhost:5000":       "localhost:5000",
		"  ghcr.io  ":                 "ghcr.io",
		"registry.example.com":        "registry.example.com",
		"registry.example.com/path":   "registry.example.com",
	}
	for in, want := range cases {
		if got := normalizeHost(in); got != want {
			t.Errorf("normalizeHost(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---------- network completion driven through upfake ----------
//
// These tests must NOT spin up an in-process HTTP registry. Instead they
// substitute listTagsFn / listCatalogFn for the duration of the test with
// thin adapters that delegate to an upfake-backed dkpreg.Client. That keeps
// the suite hermetic (no localhost ports, no real HTTP, no transport
// machinery) and immune to timeout flake when the broader `go test ./...`
// run loads the box.

func newTestCmd() *cobra.Command {
	// completion functions read --insecure off cmd.Flags(); a leaf cobra.Command
	// with that flag declared is enough for the tests.
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Bool("insecure", true, "")
	cmd.Flags().String("platform", "", "")
	_ = cmd.ParseFlags([]string{"--insecure"})
	return cmd
}

// fakeRegistryHarness wires the package-level listTagsFn / listCatalogFn
// to a dkpreg.Client backed by an in-memory upfake.Registry. Each tag and
// catalog request is also recorded so tests can assert on call counts and
// the exact host/repo references that production code constructed.
//
// The fake registry's host MUST match the host the production code asks
// about; mismatched hosts surface as "registry not configured" errors,
// which is exactly the failure mode tryListTags / tryListCatalog must
// swallow without crashing the shell.
type fakeRegistryHarness struct {
	host    string
	client  dkpreg.Client
	mu      sync.Mutex
	tagRefs []string // repoRef args passed to listTagsFn
	catRefs []string // regRef  args passed to listCatalogFn
}

// installFakeRegistry creates a fresh upfake-backed registry seeded with
// the supplied repository -> tag list mapping, and installs adapter
// implementations of listTagsFn / listCatalogFn for the duration of t.
//
// Call shape:
//
//	h := installFakeRegistry(t, "registry.example.com", map[string][]string{
//	    "foo/bar": {"v1", "v2", "latest"},
//	})
//
// The harness restores the production wiring on t.Cleanup.
func installFakeRegistry(t *testing.T, host string, repoTags map[string][]string) *fakeRegistryHarness {
	t.Helper()

	reg := upfake.NewRegistry(host)
	// One reusable empty image is enough for completion-time tests: only
	// repository names and tag strings are read by the completer.
	img := upfake.NewImageBuilder().MustBuild()
	for repo, tags := range repoTags {
		for _, tag := range tags {
			reg.MustAddImage(repo, tag, img)
		}
	}

	h := &fakeRegistryHarness{
		host:   host,
		client: upfake.NewClient(reg),
	}

	origTags := listTagsFn
	origCat := listCatalogFn

	listTagsFn = func(ctx context.Context, repoRef string, _ *registry.Options, visit func([]string) error) error {
		h.mu.Lock()
		h.tagRefs = append(h.tagRefs, repoRef)
		h.mu.Unlock()

		scoped, err := h.scope(repoRef)
		if err != nil {
			return err
		}
		tags, err := scoped.ListTags(ctx)
		if err != nil {
			return err
		}
		return visit(tags)
	}
	listCatalogFn = func(ctx context.Context, regRef string, _ *registry.Options, visit func([]string) error) error {
		h.mu.Lock()
		h.catRefs = append(h.catRefs, regRef)
		h.mu.Unlock()

		scoped, err := h.scope(regRef)
		if err != nil {
			return err
		}
		repos, err := scoped.ListRepositories(ctx)
		if err != nil {
			return err
		}
		return visit(repos)
	}

	t.Cleanup(func() {
		listTagsFn = origTags
		listCatalogFn = origCat
	})

	return h
}

// scope translates a "host[/repo/path]" string into a dkpreg.Client whose
// currentPath matches that location. Refs whose host does not match the
// fake registry's host return an error so the production code's failure
// path is exercised.
func (h *fakeRegistryHarness) scope(ref string) (dkpreg.Client, error) {
	host, repo, _ := strings.Cut(ref, "/")
	if host == "" {
		return nil, fmt.Errorf("fakeRegistryHarness: empty registry reference %q", ref)
	}
	if host != h.host {
		return nil, fmt.Errorf("fakeRegistryHarness: host %q is not configured (want %q)", host, h.host)
	}
	if repo == "" {
		// WithSegment() with no args scopes to the host root; ListRepositories
		// then returns every registered repo.
		return h.client.WithSegment(), nil
	}
	return h.client.WithSegment(strings.Split(repo, "/")...), nil
}

// tagCalls / catCalls return defensive copies of the recorded refs.
func (h *fakeRegistryHarness) tagCalls() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.tagRefs))
	copy(out, h.tagRefs)
	return out
}

func (h *fakeRegistryHarness) catCalls() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.catRefs))
	copy(out, h.catRefs)
	return out
}

// installForbidNetwork swaps both registry indirections for stubs that
// fail the test if invoked. Use to assert "the production code must not
// have touched the registry layer at all" (digest refs, ls-with-tag).
func installForbidNetwork(t *testing.T) {
	t.Helper()
	origTags := listTagsFn
	origCat := listCatalogFn
	listTagsFn = func(_ context.Context, repoRef string, _ *registry.Options, _ func([]string) error) error {
		t.Errorf("listTagsFn must not be called for this case (got repoRef=%q)", repoRef)
		return errors.New("listTagsFn called unexpectedly")
	}
	listCatalogFn = func(_ context.Context, regRef string, _ *registry.Options, _ func([]string) error) error {
		t.Errorf("listCatalogFn must not be called for this case (got regRef=%q)", regRef)
		return errors.New("listCatalogFn called unexpectedly")
	}
	t.Cleanup(func() {
		listTagsFn = origTags
		listCatalogFn = origCat
	})
}

func TestImageRef_Tags(t *testing.T) {
	const host = "registry.example.com"
	h := installFakeRegistry(t, host, map[string][]string{
		"foo/bar": {"v1", "v2", "latest"},
	})

	cmd := newTestCmd()
	got, dir := ImageRef()(cmd, nil, host+"/foo/bar:")

	want := []string{
		host + "/foo/bar:latest",
		host + "/foo/bar:v1",
		host + "/foo/bar:v2",
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("tags:\n got  %v\n want %v", got, want)
	}
	if dir&cobra.ShellCompDirectiveNoFileComp == 0 {
		t.Errorf("expected NoFileComp directive, got %v", dir)
	}
	// Production must forward the parsed host+repo (not the user-typed
	// trailing ':') to the registry layer, exactly once.
	if calls := h.tagCalls(); !slices.Equal(calls, []string{host + "/foo/bar"}) {
		t.Errorf("tagCalls = %v, want [%q]", calls, host+"/foo/bar")
	}
	if calls := h.catCalls(); len(calls) != 0 {
		t.Errorf("ImageRef tag completion must not list the catalog, got %v", calls)
	}
}

func TestImageRef_TagPrefix(t *testing.T) {
	const host = "registry.example.com"
	h := installFakeRegistry(t, host, map[string][]string{
		"foo": {"v1", "v2", "rc"},
	})

	cmd := newTestCmd()
	got, _ := ImageRef()(cmd, nil, host+"/foo:v")

	want := []string{host + "/foo:v1", host + "/foo:v2"}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("v-prefixed tags:\n got  %v\n want %v", got, want)
	}
	if calls := h.tagCalls(); !slices.Equal(calls, []string{host + "/foo"}) {
		t.Errorf("tagCalls = %v, want [%q]", calls, host+"/foo")
	}
}

func TestRepoRef_Catalog(t *testing.T) {
	const host = "registry.example.com"
	h := installFakeRegistry(t, host, map[string][]string{
		"alpha": {"1"},
		"beta":  {"1"},
	})

	cmd := newTestCmd()
	got, dir := RepoRef()(cmd, nil, host+"/")

	want := []string{host + "/alpha", host + "/beta"}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("catalog:\n got  %v\n want %v", got, want)
	}
	if dir&cobra.ShellCompDirectiveNoSpace == 0 {
		t.Errorf("expected NoSpace directive, got %v", dir)
	}
	if calls := h.catCalls(); !slices.Equal(calls, []string{host}) {
		t.Errorf("catCalls = %v, want [%q]", calls, host)
	}
}

func TestRepoRef_NoTagsForLs(t *testing.T) {
	// `cr ls REPO` does not accept a tag - if the user typed ':',
	// completion must short-circuit BEFORE touching the registry layer:
	// listTagsFn / listCatalogFn must never be invoked.
	installForbidNetwork(t)

	cmd := newTestCmd()
	got, _ := RepoRef()(cmd, nil, "registry.example.com/foo:")
	if len(got) != 0 {
		t.Errorf("RepoRef must not offer tags, got %v", got)
	}
}

// Once the user types '@' the completer must short-circuit: no suggestions
// AND no registry calls. installForbidNetwork would `t.Errorf` if either
// indirection were touched, so the assertion is implicit.
func TestImageRef_DigestRefMakesNoNetworkCall(t *testing.T) {
	installForbidNetwork(t)

	cmd := newTestCmd()
	got, dir := ImageRef()(cmd, nil, "registry.example.com/repo@sha256:")

	if len(got) != 0 {
		t.Errorf("digest ref should yield no suggestions, got: %v", got)
	}
	if dir&cobra.ShellCompDirectiveNoFileComp == 0 {
		t.Errorf("expected NoFileComp, got %v", dir)
	}
}

func TestImageRef_UnreachableHostNoCrash(t *testing.T) {
	// An unreachable registry must degrade silently to no suggestions
	// (not a stack trace in the user's terminal). The fake is configured
	// for "configured.example.com" but the user types a different host -
	// scope() returns a "host not configured" error, which tryListTags
	// must swallow.
	h := installFakeRegistry(t, "configured.example.com", nil)

	cmd := newTestCmd()
	got, dir := ImageRef()(cmd, nil, "unreachable.example.com/anything:")
	if len(got) != 0 {
		t.Errorf("expected empty suggestions on unreachable registry, got %v", got)
	}
	if dir&cobra.ShellCompDirectiveNoFileComp == 0 {
		t.Errorf("expected NoFileComp on failure, got %v", dir)
	}
	if calls := h.tagCalls(); !slices.Equal(calls, []string{"unreachable.example.com/anything"}) {
		t.Errorf("tagCalls = %v, want [%q]", calls, "unreachable.example.com/anything")
	}
}

// ---------- static / position-aware completers ----------

func TestStatic(t *testing.T) {
	got, dir := Static("a", "ab", "b")(nil, nil, "a")
	want := []string{"a", "ab"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
	if dir != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("got dir %v want NoFileComp", dir)
	}
}

func TestPathThenImage(t *testing.T) {
	cmd := newTestCmd()
	// First positional - file completion (default directive, empty list).
	got, dir := PathThenImage()(cmd, nil, "")
	if len(got) != 0 {
		t.Errorf("first arg should be file-completed (empty list), got %v", got)
	}
	if dir != cobra.ShellCompDirectiveDefault {
		t.Errorf("first arg dir = %v, want Default", dir)
	}
	// Second positional - falls through to ImageRef which (with empty
	// toComplete) suggests known registries from docker config; we only
	// assert the directive class here.
	_, dir = PathThenImage()(cmd, []string{"some-path"}, "")
	if dir&cobra.ShellCompDirectiveNoFileComp == 0 {
		t.Errorf("second arg dir should set NoFileComp, got %v", dir)
	}
}

func TestImageThenInImagePath(t *testing.T) {
	cmd := newTestCmd()
	// Once IMAGE is in args, in-image PATH completion is intentionally
	// suppressed (NoFileComp, empty list) - we do NOT want misleading
	// local-file suggestions for an in-image path.
	got, dir := ImageThenInImagePath()(cmd, []string{"some/img:tag"}, "/etc")
	if len(got) != 0 {
		t.Errorf("in-image PATH must not offer suggestions, got %v", got)
	}
	if dir != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("in-image PATH dir = %v, want NoFileComp", dir)
	}
}

// loadDockerConfigRegistries must honor $DOCKER_CONFIG just like
// authn.DefaultKeychain does at runtime - otherwise CI containers and
// rootless setups (which override $DOCKER_CONFIG) would see empty host
// suggestions while `cr` itself authenticates fine.
func TestLoadDockerConfigRegistries_HonorsDOCKER_CONFIG(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dir)

	const cfg = `{
		"auths": {
			"https://index.docker.io/v1/": {},
			"ghcr.io": {}
		},
		"credHelpers": {
			"registry.example.com": "store"
		}
	}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config.json: %v", err)
	}

	got := loadDockerConfigRegistries()
	want := []string{"ghcr.io", "index.docker.io", "registry.example.com"}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("hosts:\n got  %v\n want %v", got, want)
	}
}

func TestLoadDockerConfigRegistries_MissingConfigDegrades(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir()) // empty dir, no config.json
	if got := loadDockerConfigRegistries(); len(got) != 0 {
		t.Errorf("expected empty list when config.json is missing, got %v", got)
	}
}
