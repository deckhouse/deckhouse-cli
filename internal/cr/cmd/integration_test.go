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

// Hermetic integration tests for `d8 cr` against an in-memory OCI registry
// (httptest.NewServer + pkg/registry). No docker, no network.
package cr_test

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"

	cr "github.com/deckhouse/deckhouse-cli/internal/cr/cmd"
)

var sha256Re = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// ---------- env ----------

type testEnv struct {
	Host    string
	Alpine  string
	Alpine2 string
	Busybox string
}

func setupEnv(t *testing.T) *testEnv {
	t.Helper()
	srv := httptest.NewServer(registry.New())
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")
	env := &testEnv{
		Host:    host,
		Alpine:  host + "/alpine:3.19",
		Alpine2: host + "/alpine:3.18",
		Busybox: host + "/busybox:1.36",
	}
	pushImage(t, env.Alpine, alpineImage(t, "3.19"))
	pushImage(t, env.Alpine2, alpineImage(t, "3.18"))
	pushImage(t, env.Busybox, busyboxImage(t))
	return env
}

func pushImage(t *testing.T, refStr string, img v1.Image) {
	t.Helper()
	ref, err := name.ParseReference(refStr, name.Insecure)
	if err != nil {
		t.Fatalf("parse %s: %v", refStr, err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatalf("push %s: %v", refStr, err)
	}
}

// ---------- runner ----------

func runCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := cr.NewCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"--insecure"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

func mustRun(t *testing.T, args ...string) string {
	t.Helper()
	out, err := runCmd(t, args...)
	if err != nil {
		t.Fatalf("d8 cr %s\nerror: %v\noutput:\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func mustFail(t *testing.T, args ...string) {
	t.Helper()
	if out, err := runCmd(t, args...); err == nil {
		t.Fatalf("expected failure from: d8 cr %s\noutput:\n%s", strings.Join(args, " "), out)
	}
}

// ---------- image builders ----------

type tarFile struct {
	name     string
	typeflag byte
	content  []byte
	linkname string
	mode     int64
}

func tarLayer(t *testing.T, files []tarFile) v1.Layer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range files {
		mode := f.mode
		if mode == 0 {
			if f.typeflag == tar.TypeDir {
				mode = 0o755
			} else {
				mode = 0o644
			}
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: f.name, Typeflag: f.typeflag, Size: int64(len(f.content)),
			Linkname: f.linkname, Mode: mode,
		}); err != nil {
			t.Fatalf("tar header %s: %v", f.name, err)
		}
		if len(f.content) > 0 {
			if _, err := tw.Write(f.content); err != nil {
				t.Fatalf("tar write %s: %v", f.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	data := buf.Bytes()
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})
	if err != nil {
		t.Fatalf("layer: %v", err)
	}
	return layer
}

func buildImage(t *testing.T, layers [][]tarFile) v1.Image {
	t.Helper()
	img := empty.Image
	for _, files := range layers {
		var err error
		img, err = mutate.AppendLayers(img, tarLayer(t, files))
		if err != nil {
			t.Fatalf("append layer: %v", err)
		}
	}
	return img
}

// alpineImage: small alpine-like rootfs with /etc/os-release, /etc/passwd,
// /bin/busybox, and absolute symlinks /bin/sh and /usr/bin/awk → /bin/busybox.
// The absolute symlinks exercise the extractor's rewrite-to-relative path.
func alpineImage(t *testing.T, version string) v1.Image {
	osRelease := fmt.Sprintf("NAME=\"Alpine Linux\"\nID=alpine\nVERSION_ID=%s\nPRETTY_NAME=\"Alpine Linux v%s\"\n", version, version)
	return buildImage(t, [][]tarFile{{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "etc/", typeflag: tar.TypeDir},
		{name: "usr/", typeflag: tar.TypeDir},
		{name: "usr/bin/", typeflag: tar.TypeDir},
		{name: "bin/busybox", typeflag: tar.TypeReg, content: []byte("#!busybox"), mode: 0o755},
		{name: "etc/os-release", typeflag: tar.TypeReg, content: []byte(osRelease)},
		{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("root:x:0:0:root:/root:/bin/sh\n")},
		{name: "bin/sh", typeflag: tar.TypeSymlink, linkname: "/bin/busybox"},
		{name: "usr/bin/awk", typeflag: tar.TypeSymlink, linkname: "/bin/busybox"},
	}})
}

func busyboxImage(t *testing.T) v1.Image {
	return buildImage(t, [][]tarFile{{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/busybox", typeflag: tar.TypeReg, content: []byte("#!busybox"), mode: 0o755},
	}})
}

// ---------- file helpers ----------

func isTar(t *testing.T, path string) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	// Require at least one successful header read - an empty file would
	// otherwise return io.EOF on the first Next() call and falsely pass
	// as a "valid tar".
	if _, err := tar.NewReader(f).Next(); err != nil {
		return false
	}
	return true
}

func readJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var v map[string]any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return v
}

func parseJSON(t *testing.T, raw string) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("parse json: %v\nraw: %s", err, raw)
	}
	return v
}

// =====================================================================
// catalog / ls / manifest / config / digest
// =====================================================================

func TestIntegration_Registry(t *testing.T) {
	env := setupEnv(t)

	t.Run("catalog lists seeded repos", func(t *testing.T) {
		out := mustRun(t, "catalog", env.Host)
		for _, repo := range []string{"alpine", "busybox"} {
			if !strings.Contains(out, repo+"\n") {
				t.Fatalf("missing %q in: %s", repo, out)
			}
		}
	})
	t.Run("ls returns tag", func(t *testing.T) {
		if out := mustRun(t, "ls", env.Host+"/alpine"); !strings.Contains(out, "3.19\n") {
			t.Fatalf("missing 3.19 in: %s", out)
		}
	})
	t.Run("ls --full-ref formats host/repo:tag", func(t *testing.T) {
		if out := mustRun(t, "ls", "--full-ref", env.Host+"/alpine"); !strings.Contains(out, env.Host+"/alpine:") {
			t.Fatalf("missing full ref in: %s", out)
		}
	})
	t.Run("manifest is valid JSON with mediaType", func(t *testing.T) {
		if _, ok := parseJSON(t, mustRun(t, "manifest", env.Alpine))["mediaType"]; !ok {
			t.Fatal("manifest has no mediaType")
		}
	})
	t.Run("config has architecture", func(t *testing.T) {
		if _, ok := parseJSON(t, mustRun(t, "config", env.Alpine))["architecture"]; !ok {
			t.Fatal("config has no architecture")
		}
	})
	t.Run("digest is sha256:64hex", func(t *testing.T) {
		if d := strings.TrimSpace(mustRun(t, "digest", env.Alpine)); !sha256Re.MatchString(d) {
			t.Fatalf("bad digest: %q", d)
		}
	})
	t.Run("digest --full-ref returns repo@sha256:...", func(t *testing.T) {
		if out := mustRun(t, "digest", "--full-ref", env.Alpine); !strings.Contains(out, env.Host+"/alpine@sha256:") {
			t.Fatalf("missing full ref in: %s", out)
		}
	})
}

// =====================================================================
// pull (tarball / legacy / oci / multi)
// =====================================================================

func TestIntegration_Pull(t *testing.T) {
	env := setupEnv(t)
	work := t.TempDir()

	t.Run("tarball (default)", func(t *testing.T) {
		dst := filepath.Join(work, "alpine.tar")
		mustRun(t, "pull", env.Alpine, dst)
		if !isTar(t, dst) {
			t.Fatalf("%s is not a tar", dst)
		}
	})
	t.Run("legacy", func(t *testing.T) {
		dst := filepath.Join(work, "alpine-legacy.tar")
		mustRun(t, "pull", "--format", "legacy", env.Alpine, dst)
		if !isTar(t, dst) {
			t.Fatalf("%s is not a tar", dst)
		}
	})
	t.Run("oci layout", func(t *testing.T) {
		dst := filepath.Join(work, "oci")
		mustRun(t, "pull", "--format", "oci", env.Alpine, dst)
		if _, err := os.Stat(filepath.Join(dst, "oci-layout")); err != nil {
			t.Fatalf("missing oci-layout marker: %v", err)
		}
		if _, ok := readJSON(t, filepath.Join(dst, "index.json"))["manifests"]; !ok {
			t.Fatal("index.json has no manifests")
		}
	})
	t.Run("two images into one tarball", func(t *testing.T) {
		dst := filepath.Join(work, "multi.tar")
		mustRun(t, "pull", env.Alpine, env.Busybox, dst)
		if !isTar(t, dst) {
			t.Fatal("not a tar")
		}
	})
	t.Run("two images into oci layout", func(t *testing.T) {
		dst := filepath.Join(work, "multi-oci")
		mustRun(t, "pull", "--format", "oci", env.Alpine, env.Alpine2, dst)
		idx := readJSON(t, filepath.Join(dst, "index.json"))
		manifests, ok := idx["manifests"].([]any)
		if !ok {
			t.Fatalf("manifests is not an array: %T (full index: %v)", idx["manifests"], idx)
		}
		if len(manifests) != 2 {
			t.Fatalf("expected 2 manifests, got %d", len(manifests))
		}
	})
}

// =====================================================================
// export (crane-compatible tar stream)
// =====================================================================

func TestIntegration_Export(t *testing.T) {
	env := setupEnv(t)
	work := t.TempDir()

	t.Run("default destination is stdout", func(t *testing.T) {
		out := mustRun(t, "export", env.Alpine)
		// Output is a tar stream — first entry must be readable.
		tr := tar.NewReader(strings.NewReader(out))
		hdr, err := tr.Next()
		if err != nil {
			t.Fatalf("tar reader: %v", err)
		}
		if hdr == nil || hdr.Name == "" {
			t.Fatalf("empty header: %+v", hdr)
		}
	})

	t.Run("explicit '-' writes to stdout", func(t *testing.T) {
		out := mustRun(t, "export", env.Alpine, "-")
		if _, err := tar.NewReader(strings.NewReader(out)).Next(); err != nil {
			t.Fatalf("not a tar: %v", err)
		}
	})

	t.Run("file destination writes valid tar", func(t *testing.T) {
		dst := filepath.Join(work, "fs.tar")
		mustRun(t, "export", env.Alpine, dst)
		if !isTar(t, dst) {
			t.Fatalf("%s is not a tar archive", dst)
		}
	})

	// On any error path the destination must not be left as a half-written
	// or empty tar that could be picked up by mistake (`tar tf` happily
	// reads truncated streams). Fetch failure is the easiest way to trigger
	// the error path through the CLI surface; the corresponding cleanup is
	// in runExport.
	t.Run("file destination is removed on Fetch error", func(t *testing.T) {
		dst := filepath.Join(work, "should-not-exist.tar")
		if _, err := runCmd(t, "export", env.Host+"/no-such-image:tag", dst); err == nil {
			t.Fatal("expected fetch failure")
		}
		if _, err := os.Stat(dst); !os.IsNotExist(err) {
			t.Errorf("dst must not exist after error, Stat err=%v", err)
		}
	})

	t.Run("verbatim symlinks (absolute targets preserved)", func(t *testing.T) {
		// crane semantics: linknames are NOT rewritten. /bin/sh stays as
		// "/bin/busybox" (vs. our `fs extract` which rewrites to relative).
		dst := filepath.Join(work, "verbatim.tar")
		mustRun(t, "export", env.Alpine, dst)

		f, err := os.Open(dst)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer f.Close()

		tr := tar.NewReader(f)
		var found bool
		for {
			hdr, err := tr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("tar.Next: %v", err)
			}
			if hdr.Typeflag == tar.TypeSymlink && strings.Contains(hdr.Name, "bin/sh") {
				if hdr.Linkname != "/bin/busybox" {
					t.Fatalf("expected verbatim '/bin/busybox', got %q", hdr.Linkname)
				}
				found = true
			}
		}
		if !found {
			t.Fatal("bin/sh symlink not found in export tar")
		}
	})
}

// =====================================================================
// digest --tarball
// =====================================================================

func TestIntegration_DigestTarball(t *testing.T) {
	env := setupEnv(t)
	work := t.TempDir()
	tarPath := filepath.Join(work, "alpine.tar")
	mustRun(t, "pull", env.Alpine, tarPath)

	if d := strings.TrimSpace(mustRun(t, "digest", "--tarball", tarPath, env.Alpine)); !sha256Re.MatchString(d) {
		t.Fatalf("bad digest: %q", d)
	}
}

// =====================================================================
// fs ls / cat / tree / info / extract
// =====================================================================

func TestIntegration_FS(t *testing.T) {
	env := setupEnv(t)
	work := t.TempDir()

	t.Run("ls merged is non-empty", func(t *testing.T) {
		if out := mustRun(t, "fs", "ls", env.Alpine); strings.TrimSpace(out) == "" {
			t.Fatal("empty stdout")
		}
	})
	t.Run("ls etc lists passwd", func(t *testing.T) {
		if !strings.Contains(mustRun(t, "fs", "ls", env.Alpine, "etc"), "passwd") {
			t.Fatal("missing passwd")
		}
	})
	t.Run("ls strict path-scope etc/passwd", func(t *testing.T) {
		if !strings.Contains(mustRun(t, "fs", "ls", env.Alpine, "etc/passwd"), "passwd") {
			t.Fatal("missing passwd")
		}
	})
	t.Run("cat /etc/os-release returns Alpine", func(t *testing.T) {
		if !strings.Contains(mustRun(t, "fs", "cat", env.Alpine, "/etc/os-release"), "Alpine") {
			t.Fatal("missing 'Alpine'")
		}
	})
	t.Run("tree -L 1", func(t *testing.T) {
		if out := mustRun(t, "fs", "tree", env.Alpine, "etc", "-L", "1"); strings.TrimSpace(out) == "" {
			t.Fatal("empty tree")
		}
	})
	t.Run("extract -o materializes /etc/os-release", func(t *testing.T) {
		dst := filepath.Join(work, "rootfs")
		mustRun(t, "fs", "extract", env.Alpine, "-o", dst)
		if _, err := os.Stat(filepath.Join(dst, "etc/os-release")); err != nil {
			t.Fatalf("missing /etc/os-release: %v", err)
		}
	})
	t.Run("extract rewrites abs symlinks (alpine /bin/sh → busybox)", func(t *testing.T) {
		dst := filepath.Join(work, "rootfs-symlinks")
		mustRun(t, "fs", "extract", env.Alpine, "-o", dst)
		got, err := os.Readlink(filepath.Join(dst, "bin/sh"))
		if err != nil {
			t.Fatalf("readlink: %v", err)
		}
		if filepath.IsAbs(got) {
			t.Fatalf("expected relative target, got absolute %q", got)
		}
	})
}

// =====================================================================
// error paths
// =====================================================================

func TestIntegration_Errors(t *testing.T) {
	env := setupEnv(t)
	work := t.TempDir()
	tarPath := filepath.Join(work, "any.tar")
	mustRun(t, "pull", env.Alpine, tarPath)

	cases := []struct {
		name string
		args []string
	}{
		{"pull --format invalid", []string{"pull", "--format", "invalid", env.Alpine, filepath.Join(work, "x.tar")}},
		{"pull without PATH", []string{"pull", env.Alpine}},
		{"fs cat missing file", []string{"fs", "cat", env.Alpine, "/no/such/file"}},
		{"fs extract without -o", []string{"fs", "extract", env.Alpine}},
		{"digest without IMAGE", []string{"digest"}},
		{"digest --full-ref + --tarball", []string{"digest", "--full-ref", "--tarball", tarPath}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { mustFail(t, c.args...) })
	}

	// `digest --full-ref` without IMAGE and without --tarball must report the
	// missing-IMAGE problem, not a misleading "cannot be combined with --tarball"
	// message - the order of validation checks in digest.go matters here.
	t.Run("digest --full-ref without IMAGE blames the missing reference", func(t *testing.T) {
		out, err := runCmd(t, "digest", "--full-ref")
		if err == nil {
			t.Fatalf("expected failure, got nil")
		}
		msg := err.Error() + "\n" + out
		if strings.Contains(msg, "cannot be combined with --tarball") {
			t.Fatalf("error mentions --tarball even though it was not passed: %s", msg)
		}
		if !strings.Contains(msg, "image reference required") {
			t.Fatalf("expected 'image reference required' in error, got: %s", msg)
		}
	})
}

// =====================================================================
// push round-trip
// =====================================================================

func TestIntegration_PushRoundTrip(t *testing.T) {
	env := setupEnv(t)
	work := t.TempDir()
	tarPath := filepath.Join(work, "alpine.tar")
	mustRun(t, "pull", env.Alpine, tarPath)

	t.Run("push tarball", func(t *testing.T) {
		mustRun(t, "push", tarPath, env.Host+"/alpine:copied")
		out := mustRun(t, "ls", env.Host+"/alpine")
		if !strings.Contains(out, "copied\n") {
			t.Fatalf("copied tag missing in: %q", out)
		}
	})
	t.Run("round-trip digest matches source", func(t *testing.T) {
		src := mustRun(t, "digest", env.Alpine)
		dst := mustRun(t, "digest", env.Host+"/alpine:copied")
		if src != dst {
			t.Fatalf("digest mismatch:\n  src: %q\n  dst: %q", src, dst)
		}
	})
	t.Run("push OCI layout (single image)", func(t *testing.T) {
		oci := filepath.Join(work, "oci-single")
		mustRun(t, "pull", "--format", "oci", env.Alpine, oci)
		mustRun(t, "push", oci, env.Host+"/alpine:from-oci")
	})
	t.Run("push --image-refs writes file", func(t *testing.T) {
		refs := filepath.Join(work, "refs.txt")
		mustRun(t, "push", "--image-refs", refs, tarPath, env.Host+"/alpine:withrefs")
		info, err := os.Stat(refs)
		if err != nil || info.Size() == 0 {
			t.Fatalf("refs file empty/missing: %v", err)
		}
	})
	t.Run("push multi-image OCI without --index fails", func(t *testing.T) {
		multi := filepath.Join(work, "oci-multi")
		mustRun(t, "pull", "--format", "oci", env.Alpine, env.Alpine2, multi)
		mustFail(t, "push", multi, env.Host+"/alpine:multi")
	})
	t.Run("push multi-image OCI with --index produces index manifest", func(t *testing.T) {
		multi := filepath.Join(work, "oci-multi-ok")
		mustRun(t, "pull", "--format", "oci", env.Alpine, env.Alpine2, multi)
		mustRun(t, "push", "--index", multi, env.Host+"/alpine:multi")
		mt, _ := parseJSON(t, mustRun(t, "manifest", env.Host+"/alpine:multi"))["mediaType"].(string)
		if !strings.Contains(mt, "index") && !strings.Contains(mt, "manifest.list") {
			t.Fatalf("expected index/manifest.list, got %q", mt)
		}
	})
}
