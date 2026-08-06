//go:build windows

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

package archive_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

func TestRootedSourceRejectsWindowsLinksAndParentReplacement(t *testing.T) {
	root := t.TempDir()
	manifests := filepath.Join(root, archive.ManifestsDirName)
	if err := os.Mkdir(manifests, 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	manifest := filepath.Join(manifests, "configmap_app.yaml")
	if err := os.WriteFile(manifest, []byte("inside"), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	source, err := archive.OpenRootedSource(root)
	if err != nil {
		t.Fatalf("OpenRootedSource: %v", err)
	}
	defer func() { _ = source.Close() }()

	manifestSource, err := source.OpenDirectory(archive.ManifestsDirName)
	if err != nil {
		t.Fatalf("OpenDirectory: %v", err)
	}
	defer func() { _ = manifestSource.Close() }()

	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "configmap_app.yaml"), []byte("escaped"), 0o600); err != nil {
		t.Fatalf("write outside manifest: %v", err)
	}

	original := manifests + ".pinned-original"
	if err := os.Rename(manifests, original); err != nil {
		t.Fatalf("rename manifests: %v", err)
	}

	if err := os.Symlink(outside, manifests); err != nil {
		_ = os.Rename(original, manifests)
		t.Skipf("directory symlinks are unavailable: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Remove(manifests)
		_ = os.Rename(original, manifests)
	})

	file, err := manifestSource.OpenRegularFile("configmap_app.yaml")
	if file != nil {
		_ = file.Close()
	}

	if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
		t.Fatalf("OpenRegularFile error = %v, want ErrNonRegularArchiveArtifact", err)
	}
}

func TestRootedSourceRejectsWindowsJunctionBoundaries(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T) error
	}{
		{
			name: "archive root",
			run: func(t *testing.T) error {
				t.Helper()

				parent := t.TempDir()
				root := filepath.Join(parent, "archive")
				createWindowsJunction(t, root, t.TempDir())

				source, err := archive.OpenRootedSource(root)
				if source != nil {
					_ = source.Close()
				}

				return err
			},
		},
		{
			name: "intermediate directory",
			run: func(t *testing.T) error {
				t.Helper()

				root := t.TempDir()
				source, err := archive.OpenRootedSource(root)
				if err != nil {
					t.Fatalf("OpenRootedSource: %v", err)
				}
				defer func() { _ = source.Close() }()

				createWindowsJunction(t, filepath.Join(root, archive.ManifestsDirName), t.TempDir())

				child, err := source.OpenDirectory(archive.ManifestsDirName)
				if child != nil {
					_ = child.Close()
				}

				return err
			},
		},
		{
			name: "final file",
			run: func(t *testing.T) error {
				t.Helper()

				root := t.TempDir()
				source, err := archive.OpenRootedSource(root)
				if err != nil {
					t.Fatalf("OpenRootedSource: %v", err)
				}
				defer func() { _ = source.Close() }()

				createWindowsJunction(t, filepath.Join(root, archive.SnapshotYAMLName), t.TempDir())

				file, err := source.OpenRegularFile(archive.SnapshotYAMLName)
				if file != nil {
					_ = file.Close()
				}

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(t)
			if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Fatalf("junction open error = %v, want ErrNonRegularArchiveArtifact", err)
			}
		})
	}
}

func createWindowsJunction(t *testing.T, link, target string) {
	t.Helper()

	output, err := exec.Command("cmd.exe", "/c", "mklink", "/J", link, target).CombinedOutput()
	if err != nil {
		t.Fatalf("create junction %s -> %s: %v: %s", link, target, err, output)
	}

	t.Cleanup(func() {
		_ = os.Remove(link)
	})
}
