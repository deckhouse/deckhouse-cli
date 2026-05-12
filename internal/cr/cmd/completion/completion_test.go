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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/spf13/cobra"
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

// ---------- network completion against in-memory registry ----------

func newTestCmd() *cobra.Command {
	// completion functions read --insecure off cmd.Flags(); a leaf cobra.Command
	// with that flag declared is enough for the tests.
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Bool("insecure", true, "")
	cmd.Flags().String("platform", "", "")
	_ = cmd.ParseFlags([]string{"--insecure"})
	return cmd
}

// pushImage uploads an empty image to the test registry under refStr.
func pushImage(t *testing.T, refStr string) {
	t.Helper()
	ref, err := name.ParseReference(refStr, name.Insecure)
	if err != nil {
		t.Fatalf("parse %s: %v", refStr, err)
	}
	if err := remote.Write(ref, empty.Image); err != nil {
		t.Fatalf("push %s: %v", refStr, err)
	}
}

func TestImageRef_Tags(t *testing.T) {
	srv := httptest.NewServer(registry.New())
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	pushImage(t, host+"/foo/bar:v1")
	pushImage(t, host+"/foo/bar:v2")
	pushImage(t, host+"/foo/bar:latest")

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
}

func TestImageRef_TagPrefix(t *testing.T) {
	srv := httptest.NewServer(registry.New())
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	pushImage(t, host+"/foo:v1")
	pushImage(t, host+"/foo:v2")
	pushImage(t, host+"/foo:rc")

	cmd := newTestCmd()
	got, _ := ImageRef()(cmd, nil, host+"/foo:v")

	want := []string{host + "/foo:v1", host + "/foo:v2"}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("v-prefixed tags:\n got  %v\n want %v", got, want)
	}
}

func TestRepoRef_Catalog(t *testing.T) {
	srv := httptest.NewServer(registry.New())
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	pushImage(t, host+"/alpha:1")
	pushImage(t, host+"/beta:1")

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
}

func TestRepoRef_NoTagsForLs(t *testing.T) {
	// `cr ls REPO` does not accept a tag - if the user typed ':',
	// completion must offer nothing rather than tags.
	srv := httptest.NewServer(registry.New())
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	pushImage(t, host+"/foo:v1")

	cmd := newTestCmd()
	got, _ := RepoRef()(cmd, nil, host+"/foo:")
	if len(got) != 0 {
		t.Errorf("RepoRef must not offer tags, got %v", got)
	}
}

// Once the user types '@' the completer must short-circuit: no suggestions,
// no doomed network calls. The handler counts incoming requests so the
// "no network call" half of the contract is verified directly, not by
// inferring it from an empty result.
func TestImageRef_DigestRefMakesNoNetworkCall(t *testing.T) {
	var calls int32
	inner := registry.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	cmd := newTestCmd()
	got, dir := ImageRef()(cmd, nil, host+"/repo@sha256:")

	if len(got) != 0 {
		t.Errorf("digest ref should yield no suggestions, got: %v", got)
	}
	if dir&cobra.ShellCompDirectiveNoFileComp == 0 {
		t.Errorf("expected NoFileComp, got %v", dir)
	}
	if c := atomic.LoadInt32(&calls); c != 0 {
		t.Errorf("digest ref must not contact the registry, got %d HTTP calls", c)
	}
}

func TestImageRef_UnreachableHostNoCrash(t *testing.T) {
	// An unreachable registry must degrade silently to no suggestions
	// (not a stack trace in the user's terminal). We point at a closed
	// port and assert the call returns without panicking.
	srv := httptest.NewServer(registry.New())
	srv.Close() // immediately close - subsequent calls will fail to connect
	host := strings.TrimPrefix(srv.URL, "http://")

	cmd := newTestCmd()
	got, dir := ImageRef()(cmd, nil, host+"/anything:")
	if len(got) != 0 {
		t.Errorf("expected empty suggestions on unreachable registry, got %v", got)
	}
	if dir&cobra.ShellCompDirectiveNoFileComp == 0 {
		t.Errorf("expected NoFileComp on failure, got %v", dir)
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
