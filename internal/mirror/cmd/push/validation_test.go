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

package push

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
)

// resetPushState clears every package-level flag var this file touches.
// Tests never rely on cobra flag defaults (addFlags is not called here), so
// resetting to the zero value is equivalent to a clean process start.
func resetPushState() {
	RegistryUsername = ""
	RegistryPassword = ""
	TLSSkipVerify = false
	Insecure = false
	TempDir = ""
	ModulesPathSuffix = ""
	Files = nil
	ImagesBundlePath = ""
	Packages = nil
	RegistryHost = ""
	RegistryPath = ""
	MirrorTimeout = -1
}

func TestIsPackageFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		want     bool
	}{
		{name: "tar file", fileName: "platform.tar", want: true},
		{name: "chunk file", fileName: "platform.tar.0000.chunk", want: true},
		{name: "unrelated extension", fileName: "platform.txt", want: false},
		{name: "no extension", fileName: "platform", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isPackageFile(tt.fileName))
		})
	}
}

func TestCanonicalPackagePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "plain tar path is unchanged",
			path: "/bundle/platform.tar",
			want: "/bundle/platform.tar",
		},
		{
			name: "first chunk collapses to the canonical tar path",
			path: "/bundle/platform.tar.0000.chunk",
			want: "/bundle/platform.tar",
		},
		{
			name: "later chunk index collapses to the same canonical tar path",
			path: "/bundle/platform.tar.0012.chunk",
			want: "/bundle/platform.tar",
		},
		{
			name: "chunked module archive with dashes in the name",
			path: "/bundle/module-foo.tar.0003.chunk",
			want: "/bundle/module-foo.tar",
		},
		{
			name: "non-package path is returned unchanged",
			path: "/bundle/readme.txt",
			want: "/bundle/readme.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, canonicalPackagePath(tt.path))
		})
	}
}

// TestCanonicalPackagePathConsistency documents the invariant that
// internal/mirror.packageNameFromPath relies on: every package file accepted
// by isPackageFile (plain .tar or a .chunk fragment) canonicalizes to a path
// ending in ".tar". If this ever regresses, package name derivation on the
// mirror.PushService side silently breaks for chunked archives.
func TestCanonicalPackagePathConsistency(t *testing.T) {
	inputs := []string{
		"platform.tar",
		"platform.tar.0000.chunk",
		"platform.tar.0001.chunk",
		"module-foo.tar.0012.chunk",
	}

	for _, in := range inputs {
		path := filepath.Join("/bundle", in)
		got := canonicalPackagePath(path)

		assert.Equal(t, ".tar", filepath.Ext(got), "canonicalized path %q (from %q) must end in .tar", got, path)
		assert.True(t, isPackageFile(got), "canonicalized path %q must still look like a package file", got)
	}
}

func TestCollectFilesPackages(t *testing.T) {
	tempDir := t.TempDir()
	tarFile := filepath.Join(tempDir, "a.tar")
	chunkFile := filepath.Join(tempDir, "b.tar.0000.chunk")
	txtFile := filepath.Join(tempDir, "c.txt")
	subDir := filepath.Join(tempDir, "subdir")

	require.NoError(t, os.WriteFile(tarFile, []byte("x"), 0644))
	require.NoError(t, os.WriteFile(chunkFile, []byte("x"), 0644))
	require.NoError(t, os.WriteFile(txtFile, []byte("x"), 0644))
	require.NoError(t, os.MkdirAll(subDir, 0755))

	tests := []struct {
		name         string
		files        []string
		expectError  bool
		errorMsg     string
		wantPackages []string
	}{
		{
			name:         "single tar file",
			files:        []string{tarFile},
			wantPackages: []string{tarFile},
		},
		{
			name:         "chunk file resolves to its canonical tar path",
			files:        []string{chunkFile},
			wantPackages: []string{filepath.Join(tempDir, "b.tar")},
		},
		{
			name:        "missing file",
			files:       []string{filepath.Join(tempDir, "missing.tar")},
			expectError: true,
			errorMsg:    "could not read package",
		},
		{
			name:        "directory instead of a file",
			files:       []string{subDir},
			expectError: true,
			errorMsg:    "is a directory",
		},
		{
			name:        "wrong extension",
			files:       []string{txtFile},
			expectError: true,
			errorMsg:    "not a tar or chunked package",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetPushState()
			t.Cleanup(resetPushState)

			Files = tt.files

			err := collectFilesPackages()

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantPackages, Packages)
		})
	}
}

func TestCollectBundlePathPackages(t *testing.T) {
	t.Run("directory with tar and chunked packages", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "platform.tar"), []byte("x"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "module-foo.tar.0000.chunk"), []byte("x"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "module-foo.tar.0001.chunk"), []byte("x"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("x"), 0644))

		err := collectBundlePathPackages(dir)
		require.NoError(t, err)

		// Each chunk is appended once, so the two module-foo chunks both
		// resolve to the same canonical path here. Deduping is resolvePackages's
		// job, not collectBundlePathPackages's - this is intentional, not a bug.
		assert.ElementsMatch(t, []string{
			filepath.Join(dir, "platform.tar"),
			filepath.Join(dir, "module-foo.tar"),
			filepath.Join(dir, "module-foo.tar"),
		}, Packages)
	})

	t.Run("directory without packages errors", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("x"), 0644))

		err := collectBundlePathPackages(dir)
		assert.ErrorContains(t, err, "no packages found in bundle directory")
	})

	t.Run("single tar file as bundle path", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		dir := t.TempDir()
		tarFile := filepath.Join(dir, "platform.tar")
		require.NoError(t, os.WriteFile(tarFile, []byte("x"), 0644))

		err := collectBundlePathPackages(tarFile)
		require.NoError(t, err)
		assert.Equal(t, []string{tarFile}, Packages)
	})

	t.Run("missing path errors", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		err := collectBundlePathPackages(filepath.Join(t.TempDir(), "missing"))
		assert.ErrorContains(t, err, "could not read images bundle")
	})
}

func TestResolvePackages(t *testing.T) {
	t.Run("bundle dir chunks dedup into one package and temp dir lands inside it", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "platform.tar.0000.chunk"), []byte("x"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "platform.tar.0001.chunk"), []byte("x"), 0644))

		err := resolvePackages([]string{dir})
		require.NoError(t, err)

		assert.Equal(t, []string{filepath.Join(dir, "platform.tar")}, Packages)
		assert.Equal(t, filepath.Join(dir, ".tmp", mirror.TmpMirrorFolderName), TempDir)
	})

	t.Run("single archive bundle path derives temp dir next to the file", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		dir := t.TempDir()
		tarFile := filepath.Join(dir, "platform.tar")
		require.NoError(t, os.WriteFile(tarFile, []byte("x"), 0644))

		err := resolvePackages([]string{tarFile})
		require.NoError(t, err)

		assert.Equal(t, []string{tarFile}, Packages)
		assert.Equal(t, filepath.Join(dir, ".tmp", mirror.TmpMirrorFolderName), TempDir)
	})

	t.Run("--file alone, no bundle path argument", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		dir := t.TempDir()
		tarFile := filepath.Join(dir, "platform.tar")
		require.NoError(t, os.WriteFile(tarFile, []byte("x"), 0644))
		Files = []string{tarFile}

		err := resolvePackages(nil)
		require.NoError(t, err)
		assert.Equal(t, []string{tarFile}, Packages)
	})

	t.Run("bundle dir combined with --file merges both sources", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "platform.tar"), []byte("x"), 0644))

		otherDir := t.TempDir()
		extraTar := filepath.Join(otherDir, "extra.tar")
		require.NoError(t, os.WriteFile(extraTar, []byte("x"), 0644))
		Files = []string{extraTar}

		err := resolvePackages([]string{dir})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{filepath.Join(dir, "platform.tar"), extraTar}, Packages)
	})

	t.Run("no bundle path and no --file errors", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		err := resolvePackages(nil)
		assert.ErrorContains(t, err, "no packages to push")
	})

	t.Run("explicit temp dir is not overridden by the default", func(t *testing.T) {
		resetPushState()
		t.Cleanup(resetPushState)

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "platform.tar"), []byte("x"), 0644))
		TempDir = "/custom/tmp"

		err := resolvePackages([]string{dir})
		require.NoError(t, err)
		assert.Equal(t, "/custom/tmp", TempDir)
	})
}

func TestValidateRegistryCredentials(t *testing.T) {
	tests := []struct {
		name        string
		username    string
		password    string
		expectError bool
	}{
		{name: "no credentials", username: "", password: ""},
		{name: "username and password", username: "user", password: "pass"},
		{name: "username only", username: "user", password: ""},
		{name: "password without username", username: "", password: "pass", expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetPushState()
			t.Cleanup(resetPushState)

			RegistryUsername = tt.username
			RegistryPassword = tt.password

			err := validateRegistryCredentials()
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestParseAndValidateRegistryURLArg(t *testing.T) {
	tests := []struct {
		name        string
		registry    string
		expectError bool
		errorMsg    string
		wantHost    string
		wantPath    string
	}{
		{
			name:     "valid registry with host and path",
			registry: "registry.example.com/deckhouse/ee",
			wantHost: "registry.example.com",
			wantPath: "/deckhouse/ee",
		},
		{
			name:     "https scheme is stripped",
			registry: "https://registry.example.com/deckhouse/ee",
			wantHost: "registry.example.com",
			wantPath: "/deckhouse/ee",
		},
		{
			name:        "empty registry",
			registry:    "",
			expectError: true,
			errorMsg:    "argument is empty",
		},
		{
			name:        "no repository path",
			registry:    "registry.example.com",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetPushState()
			t.Cleanup(resetPushState)

			err := parseAndValidateRegistryURLArg(tt.registry)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, RegistryHost)
			assert.Equal(t, tt.wantPath, RegistryPath)
		})
	}
}

func TestParseAndValidateParameters(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "platform.tar"), []byte("x"), 0644))

	const validRegistry = "registry.example.com/deckhouse"

	tests := []struct {
		name        string
		args        []string
		setup       func()
		expectError bool
		errorMsg    string
	}{
		{
			name: "bundle dir + registry",
			args: []string{dir, validRegistry},
		},
		{
			name: "registry only, with --file set",
			args: []string{validRegistry},
			setup: func() {
				Files = []string{filepath.Join(dir, "platform.tar")}
			},
		},
		{
			name:        "registry only, without --file errors",
			args:        []string{validRegistry},
			expectError: true,
			errorMsg:    "no packages to push",
		},
		{
			name:        "no arguments",
			args:        []string{},
			expectError: true,
			errorMsg:    "invalid number of arguments",
		},
		{
			name:        "too many arguments",
			args:        []string{dir, validRegistry, "extra"},
			expectError: true,
			errorMsg:    "invalid number of arguments",
		},
		{
			name:        "invalid registry",
			args:        []string{dir, "not a valid registry"},
			expectError: true,
		},
		{
			name: "password without username",
			args: []string{dir, validRegistry},
			setup: func() {
				RegistryPassword = "secret"
			},
			expectError: true,
			errorMsg:    "registry username not specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetPushState()
			t.Cleanup(resetPushState)

			if tt.setup != nil {
				tt.setup()
			}

			err := parseAndValidateParameters(nil, tt.args)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				return
			}
			assert.NoError(t, err)
		})
	}
}
