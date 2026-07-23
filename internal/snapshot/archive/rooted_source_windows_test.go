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
