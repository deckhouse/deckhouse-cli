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

package plugins

import (
	"archive/tar"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
)

// resetLegacyFlags clears the legacy --source globals for a test and restores
// them afterwards (they are package-level, so tests must not leak state).
func resetLegacyFlags(t *testing.T) {
	t.Helper()

	repo, login, pass := d8flags.SourceRegistryRepo, d8flags.SourceRegistryLogin, d8flags.SourceRegistryPassword
	lic, insec, tls, skip := d8flags.DeckhouseLicenseToken, d8flags.Insecure, d8flags.TLSSkipVerify, d8flags.SkipClusterChecks

	t.Cleanup(func() {
		d8flags.SourceRegistryRepo, d8flags.SourceRegistryLogin, d8flags.SourceRegistryPassword = repo, login, pass
		d8flags.DeckhouseLicenseToken, d8flags.Insecure, d8flags.TLSSkipVerify, d8flags.SkipClusterChecks = lic, insec, tls, skip
	})

	d8flags.SourceRegistryRepo, d8flags.SourceRegistryLogin, d8flags.SourceRegistryPassword = "", "", ""
	d8flags.DeckhouseLicenseToken, d8flags.Insecure, d8flags.TLSSkipVerify, d8flags.SkipClusterChecks = "", false, false, false
}

// TestRegistryPluginSourceTagsPath is the guard for the CI pipeline: a plugin
// image must be addressed as <--source>/plugins/<name>. It runs ListPluginTags
// against a fake registry and asserts both the returned tags and the exact repo
// path the client requested.
func TestRegistryPluginSourceTagsPath(t *testing.T) {
	resetLegacyFlags(t)

	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/" {
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			w.WriteHeader(http.StatusOK)

			return
		}

		if strings.HasSuffix(r.URL.Path, "/tags/list") {
			gotPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"x","tags":["v1.0.0","v1.1.0"]}`))

			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	d8flags.SourceRegistryRepo = host + "/deckhouse/deckhouse-cli"
	d8flags.Insecure = true // httptest serves plain HTTP

	source := newRegistryPluginSource(dkplog.NewNop())

	tags, err := source.ListPluginTags(context.Background(), "package")
	require.NoError(t, err)
	assert.Equal(t, []string{"v1.0.0", "v1.1.0"}, tags)
	assert.Equal(t, "/v2/deckhouse/deckhouse-cli/plugins/package/tags/list", gotPath)
}

func TestInitLegacyRegistrySourceSelectsRegistryAndSkipsCluster(t *testing.T) {
	resetLegacyFlags(t)

	d8flags.SourceRegistryRepo = "registry.example.com/deckhouse/deckhouse-cli"

	m := testManager()
	require.NoError(t, m.initLegacyRegistrySource())

	assert.IsType(t, &registryPluginSource{}, m.service)
	assert.True(t, d8flags.SkipClusterChecks, "the --source bypass must skip cluster-side checks")
}

func TestLegacyRegistryHost(t *testing.T) {
	cases := map[string]string{
		"registry.example.com":                         "registry.example.com",
		"registry.example.com/deckhouse/deckhouse-cli": "registry.example.com",
		"registry.example.com:5000/foo":                "registry.example.com:5000",
		"127.0.0.1:38574/deckhouse/deckhouse-cli":      "127.0.0.1:38574",
	}

	for source, want := range cases {
		assert.Equalf(t, want, legacyRegistryHost(source), "source %q", source)
	}
}

func TestLegacyRegistryAuthPriority(t *testing.T) {
	logger := dkplog.NewNop()

	t.Run("explicit login wins", func(t *testing.T) {
		resetLegacyFlags(t)
		d8flags.SourceRegistryLogin = "alice"
		d8flags.SourceRegistryPassword = "secret"
		d8flags.DeckhouseLicenseToken = "ignored"

		cfg, err := legacyRegistryAuth("registry.example.com", logger).Authorization()
		require.NoError(t, err)
		assert.Equal(t, "alice", cfg.Username)
		assert.Equal(t, "secret", cfg.Password)
	})

	t.Run("license token maps to license-token user", func(t *testing.T) {
		resetLegacyFlags(t)
		d8flags.DeckhouseLicenseToken = "lic-123"

		cfg, err := legacyRegistryAuth("registry.example.com", logger).Authorization()
		require.NoError(t, err)
		assert.Equal(t, "license-token", cfg.Username)
		assert.Equal(t, "lic-123", cfg.Password)
	})

	t.Run("no credentials falls back to anonymous", func(t *testing.T) {
		resetLegacyFlags(t)
		// Point the Docker keychain at an empty config so the lookup finds nothing.
		t.Setenv("DOCKER_CONFIG", t.TempDir())

		assert.Equal(t, authn.Anonymous, legacyRegistryAuth("registry.example.com", logger))
	})
}

func TestExtractPluginBinary(t *testing.T) {
	t.Run("extracts the plugin entry, forced executable", func(t *testing.T) {
		archive := plainTar(t, map[string]string{
			"README":              "docs",
			pluginBinaryEntryName: "#!/bin/sh\necho hi\n",
		})

		dest := filepath.Join(t.TempDir(), "out")
		require.NoError(t, extractPluginBinary(bytes.NewReader(archive), dest))

		content, err := os.ReadFile(dest)
		require.NoError(t, err)
		assert.Equal(t, "#!/bin/sh\necho hi\n", string(content))

		info, err := os.Stat(dest)
		require.NoError(t, err)
		assert.Equal(t, legacyExecutableMode, info.Mode().Perm())
	})

	t.Run("errors when no plugin entry is present", func(t *testing.T) {
		archive := plainTar(t, map[string]string{"other": "x"})

		err := extractPluginBinary(bytes.NewReader(archive), filepath.Join(t.TempDir(), "out"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

// plainTar builds an uncompressed tar (as an image's Extract() stream would be)
// from name->content entries.
func plainTar(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for name, content := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
		}))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())

	return buf.Bytes()
}
