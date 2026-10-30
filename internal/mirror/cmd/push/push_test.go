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

package push

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// writeLayoutTar packs an OCI layout holding one image, tagged shortTag, into
// <dir>/<tarName> under the given tar prefix, the way pull writes a bundle.
func writeLayoutTar(t *testing.T, dir, tarName, prefix, shortTag string) {
	t.Helper()

	layoutDir := t.TempDir()
	imgLayout, err := regimage.NewImageLayout(layoutDir)
	require.NoError(t, err)

	img := upfake.NewImageBuilder().WithFile("version.json", `{"version":"`+shortTag+`"}`).MustBuild()
	require.NoError(t, imgLayout.Path().AppendImage(img, layout.WithAnnotations(map[string]string{
		regimage.AnnotationImageShortTag: shortTag,
	})))

	f, err := os.Create(filepath.Join(dir, tarName))
	require.NoError(t, err)
	defer f.Close()

	require.NoError(t, bundle.PackWithPrefix(context.Background(), layoutDir, prefix, f))
}

// TestPush_TargetRepoRouting runs the push command against a live registry
// and checks where each layout lands. A target that ends with an edition keeps
// the installer and deckhouse-cli in the root above the edition, with no flag;
// any other target receives the bundle as is.
func TestPush_TargetRepoRouting(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		wantExists []string
		wantAbsent []string
	}{
		{
			name:   "edition target",
			target: "/deckhouse/ee",
			wantExists: []string{
				"deckhouse/ee:v1.76.2",
				"deckhouse/installer:latest",
				"deckhouse/deckhouse-cli:v0.13.1",
				"deckhouse/deckhouse-cli/plugins/system:v1.0.0",
				"deckhouse/deckhouse-cli/plugins:system",
			},
			wantAbsent: []string{
				"deckhouse/ee/installer:latest",
				"deckhouse/ee/deckhouse-cli:v0.13.1",
				"deckhouse/ee/deckhouse-cli/plugins/system:v1.0.0",
			},
		},
		{
			name:   "cse target",
			target: "/deckhouse/cse",
			wantExists: []string{
				"deckhouse/cse:v1.76.2",
				"deckhouse/installer:latest",
				"deckhouse/deckhouse-cli:v0.13.1",
				"deckhouse/deckhouse-cli/plugins/system:v1.0.0",
				"deckhouse/deckhouse-cli/plugins:system",
			},
			wantAbsent: []string{
				"deckhouse/cse/installer:latest",
				"deckhouse/cse/deckhouse-cli:v0.13.1",
			},
		},
		{
			name:   "edition as the only path segment",
			target: "/ee",
			wantExists: []string{
				"ee:v1.76.2",
				"ee/installer:latest",
				"ee/deckhouse-cli:v0.13.1",
				"ee/deckhouse-cli/plugins/system:v1.0.0",
			},
		},
		{
			name:   "no edition",
			target: "/mirror/deckhouse",
			wantExists: []string{
				"mirror/deckhouse:v1.76.2",
				"mirror/deckhouse/installer:latest",
				"mirror/deckhouse/deckhouse-cli:v0.13.1",
				"mirror/deckhouse/deckhouse-cli/plugins/system:v1.0.0",
			},
			wantAbsent: []string{
				"mirror/installer:latest",
				"mirror/deckhouse-cli:v0.13.1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetPushState()
			t.Cleanup(resetPushState)

			srv := httptest.NewServer(ggcrregistry.New())
			t.Cleanup(srv.Close)
			host := strings.TrimPrefix(srv.URL, "http://")

			bundleDir := t.TempDir()
			writeLayoutTar(t, bundleDir, "platform.tar", "", "v1.76.2")
			writeLayoutTar(t, bundleDir, "installer.tar", "installer", "latest")
			writeLayoutTar(t, bundleDir, "deckhouse-cli.tar", "deckhouse-cli", "v0.13.1")
			writeLayoutTar(t, bundleDir, "plugin-system.tar", path.Join("deckhouse-cli", "plugins", "system"), "v1.0.0")

			require.NoError(t, parseAndValidateParameters(nil, []string{bundleDir, host + tt.target}))
			Insecure = true

			require.NoError(t, NewPusher().Execute())

			for _, ref := range tt.wantExists {
				assert.NoErrorf(t, headImage(host+"/"+ref), "%s must be pushed", ref)
			}

			for _, ref := range tt.wantAbsent {
				assert.Errorf(t, headImage(host+"/"+ref), "%s must not be pushed", ref)
			}
		})
	}
}

func headImage(ref string) error {
	parsed, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		return err
	}

	_, err = remote.Head(parsed)

	return err
}

// TestPush_TempDirContract checks that push keeps its temporary files in
// <tmp-dir>/.d8-mirror-tmp and removes only that directory, whatever the outcome.
func TestPush_TempDirContract(t *testing.T) {
	// setup builds a bundle inside a user temp dir that also holds foreign data,
	// the way --tmp-dir /llm/ held both the bundle and a database.
	setup := func(t *testing.T, wrap func(http.Handler) http.Handler) (host, tmpDir, bundleDir, foreignFile string) {
		t.Helper()

		resetPushState()
		t.Cleanup(resetPushState)

		handler := ggcrregistry.New()
		if wrap != nil {
			handler = wrap(handler)
		}

		srv := httptest.NewServer(handler)
		t.Cleanup(srv.Close)
		host = strings.TrimPrefix(srv.URL, "http://")

		tmpDir = t.TempDir()
		foreignFile = filepath.Join(tmpDir, "db", "data.db")
		require.NoError(t, os.MkdirAll(filepath.Dir(foreignFile), 0o755))
		require.NoError(t, os.WriteFile(foreignFile, []byte("keep"), 0o644))

		bundleDir = filepath.Join(tmpDir, "upload")
		require.NoError(t, os.Mkdir(bundleDir, 0o755))
		writeLayoutTar(t, bundleDir, "platform.tar", "", "v1.76.2")

		return host, tmpDir, bundleDir, foreignFile
	}

	t.Run("success removes only the work dir", func(t *testing.T) {
		host, tmpDir, bundleDir, foreignFile := setup(t, nil)

		TempDir = tmpDir
		require.NoError(t, parseAndValidateParameters(nil, []string{bundleDir, host + "/deckhouse/ee"}))
		Insecure = true

		require.NoError(t, NewPusher().Execute())

		assert.NoError(t, headImage(host+"/deckhouse/ee:v1.76.2"))
		assert.NoDirExists(t, filepath.Join(tmpDir, workDirName))
		assert.FileExists(t, foreignFile)
		assert.FileExists(t, filepath.Join(bundleDir, "platform.tar"))
	})

	t.Run("failure removes only the work dir", func(t *testing.T) {
		host, tmpDir, bundleDir, foreignFile := setup(t, rejectManifestUploads)

		TempDir = tmpDir
		require.NoError(t, parseAndValidateParameters(nil, []string{bundleDir, host + "/deckhouse/ee"}))
		Insecure = true

		// Failing past the access check means the work dir was created and used.
		require.ErrorContains(t, NewPusher().Execute(), "push to registry")

		assert.NoDirExists(t, filepath.Join(tmpDir, workDirName))
		assert.FileExists(t, foreignFile)
		assert.FileExists(t, filepath.Join(bundleDir, "platform.tar"))
	})

	t.Run("existing work dir stops push and is left intact", func(t *testing.T) {
		host, tmpDir, bundleDir, foreignFile := setup(t, nil)
		leftover := filepath.Join(tmpDir, workDirName, "leftover")
		require.NoError(t, os.MkdirAll(filepath.Dir(leftover), 0o755))
		require.NoError(t, os.WriteFile(leftover, []byte("keep"), 0o644))

		TempDir = tmpDir
		require.NoError(t, parseAndValidateParameters(nil, []string{bundleDir, host + "/deckhouse/ee"}))
		Insecure = true

		err := NewPusher().Execute()
		require.ErrorContains(t, err, filepath.Join(tmpDir, workDirName)+" already exists and is not empty")

		assert.Error(t, headImage(host+"/deckhouse/ee:d8WriteCheck"), "the registry must not be contacted")
		assert.Error(t, headImage(host+"/deckhouse/ee:v1.76.2"), "nothing must be pushed")
		assert.FileExists(t, leftover)
		assert.FileExists(t, foreignFile)
	})

	t.Run("default temp dir is removed once empty", func(t *testing.T) {
		host, _, bundleDir, _ := setup(t, nil)

		require.NoError(t, parseAndValidateParameters(nil, []string{bundleDir, host + "/deckhouse/ee"}))
		Insecure = true

		require.NoError(t, NewPusher().Execute())

		assert.NoDirExists(t, filepath.Join(bundleDir, ".tmp"))
		assert.FileExists(t, filepath.Join(bundleDir, "platform.tar"))
	})

	// Both cancellations exit 0, like a graceful Ctrl+C, and still clean up.
	cancelTests := []struct {
		name      string
		interrupt func(*http.Request) bool
		pushed    bool
	}{
		{
			name:      "cancellation during the pre-check removes only the work dir",
			interrupt: func(*http.Request) bool { return true },
		},
		{
			name: "cancellation during the push removes only the work dir",
			interrupt: func(r *http.Request) bool {
				return r.Method == http.MethodPut && isLayoutManifestUpload(r)
			},
		},
	}

	for _, tt := range cancelTests {
		t.Run(tt.name, func(t *testing.T) {
			host, tmpDir, bundleDir, foreignFile := setup(t, func(next http.Handler) http.Handler {
				return interruptOn(t, tt.interrupt, next)
			})

			TempDir = tmpDir
			require.NoError(t, parseAndValidateParameters(nil, []string{bundleDir, host + "/deckhouse/ee"}))
			Insecure = true

			require.NoError(t, NewPusher().Execute())

			assert.Error(t, headImage(host+"/deckhouse/ee:v1.76.2"), "the interrupted manifest must not land")
			assert.NoDirExists(t, filepath.Join(tmpDir, workDirName))
			assert.FileExists(t, foreignFile)
			assert.FileExists(t, filepath.Join(bundleDir, "platform.tar"))
		})
	}
}

// isLayoutManifestUpload reports a manifest upload of the pushed bundle, as
// opposed to the write probe of the registry access check.
func isLayoutManifestUpload(r *http.Request) bool {
	return strings.Contains(r.URL.Path, "/manifests/") && !strings.HasSuffix(r.URL.Path, "/d8WriteCheck")
}

// rejectManifestUploads fails every manifest upload except the write probe of
// the registry access check, so push fails mid-run, after the work dir is populated.
func rejectManifestUploads(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && isLayoutManifestUpload(r) {
			http.Error(w, "manifest uploads are disabled", http.StatusBadRequest)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// interruptOn sends SIGINT to the test process on the first request that
// matches, the way Ctrl+C does, and fails that request once the client gives it
// up or after a short delay, whichever comes first: not every request is bound
// to the push context, and push then sees the cancellation on its next step.
// Execute is running while a request is served, so its signal handler is set
// and the signal never reaches the default action.
func interruptOn(t *testing.T, match func(*http.Request) bool, next http.Handler) http.Handler {
	t.Helper()

	var once sync.Once

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fired := false
		if match(r) {
			once.Do(func() {
				fired = true
				require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGINT))
			})
		}

		if !fired {
			next.ServeHTTP(w, r)
			return
		}

		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}

		http.Error(w, "interrupted", http.StatusServiceUnavailable)
	})
}
