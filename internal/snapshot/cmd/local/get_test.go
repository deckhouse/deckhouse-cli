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

package local_test

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sigsyaml "sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/local"
)

type localArchiveMutation string

const (
	localArchiveValid            localArchiveMutation = "valid"
	localArchiveCorruptPayload   localArchiveMutation = "corrupt payload"
	localArchiveCorruptManifest  localArchiveMutation = "corrupt manifest"
	localArchiveCorruptMetadata  localArchiveMutation = "corrupt metadata checksum"
	localArchiveMissingVersion   localArchiveMutation = "missing version"
	localArchiveExplicitVersion0 localArchiveMutation = "explicit version zero"
)

func TestLocalGetVerifiesIntegrityBeforeRendering(t *testing.T) {
	tests := []struct {
		name     string
		mutation localArchiveMutation
		wantErr  error
	}{
		{name: "valid archive", mutation: localArchiveValid},
		{name: "corrupt payload", mutation: localArchiveCorruptPayload, wantErr: archive.ErrChecksumMismatch},
		{name: "corrupt manifest", mutation: localArchiveCorruptManifest, wantErr: archive.ErrChecksumMismatch},
		{
			name:     "corrupt metadata checksum",
			mutation: localArchiveCorruptMetadata,
			wantErr:  archive.ErrSnapshotMetadataChecksumMismatch,
		},
		{name: "missing version", mutation: localArchiveMissingVersion, wantErr: archive.ErrLegacySnapshotFormat},
		{
			name:     "explicit version zero",
			mutation: localArchiveExplicitVersion0,
			wantErr:  archive.ErrLegacySnapshotFormat,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := buildLocalIntegrityArchive(t, test.mutation)
			var output bytes.Buffer

			cmd := local.NewGetCommand(discardLog())
			cmd.SetOut(&output)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{root})

			err := cmd.Execute()
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("Execute() error = %v, want %v", err, test.wantErr)
				}

				if output.Len() != 0 {
					t.Fatalf("corrupt archive rendered output before failing: %q", output.String())
				}

				return
			}

			if err != nil {
				t.Fatalf("Execute(): %v", err)
			}

			want := "Snapshot/root  namespace=default  children=0  volumes=0\n"
			if output.String() != want {
				t.Fatalf("output = %q, want %q", output.String(), want)
			}
		})
	}
}

func TestLocalGetLegacyCompatibilityIsExplicitAndWarns(t *testing.T) {
	root := buildLocalIntegrityArchive(t, localArchiveMissingVersion)
	var output bytes.Buffer
	var logs bytes.Buffer

	cmd := local.NewGetCommand(slog.New(slog.NewTextHandler(&logs, nil)))
	cmd.SetOut(&output)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--allow-unauthenticated-legacy", root})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute(): %v", err)
	}

	if !strings.Contains(output.String(), "Snapshot/root") {
		t.Fatalf("legacy output = %q, want root summary", output.String())
	}

	for _, fragment := range []string{
		"allowing unauthenticated legacy snapshot metadata",
		"cannot distinguish a genuine pre-version archive from a downgraded tampered archive",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("warning does not contain %q: %s", fragment, logs.String())
		}
	}
}

func TestLocalGetHelpDocumentsDefaultVerification(t *testing.T) {
	cmd := local.NewGetCommand(discardLog())

	for _, fragment := range []string{
		"every node's content checksum",
		"verified before any output is rendered",
		"--allow-unauthenticated-legacy",
		"cannot distinguish a genuine legacy archive",
	} {
		if !strings.Contains(cmd.Long, fragment) {
			t.Errorf("Long help does not contain %q:\n%s", fragment, cmd.Long)
		}
	}
}

func buildLocalIntegrityArchive(t *testing.T, mutation localArchiveMutation) string {
	t.Helper()

	root := t.TempDir()
	sy := archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root",
		Namespace:  "default",
	}

	switch mutation {
	case localArchiveCorruptPayload:
		if err := os.WriteFile(filepath.Join(root, archive.DataBlockBase), []byte("payload"), 0o600); err != nil {
			t.Fatalf("write payload: %v", err)
		}

		sy.Kind = "VolumeSnapshot"
		sy.Volumes = []archive.VolumeInfo{{
			Target: archive.VolumeObjectRef{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc",
			},
			Artifact: archive.VolumeObjectRef{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshotContent",
				Name:       "content",
			},
			VolumeMode:       archive.VolumeModeBlock,
			StorageClassName: "default",
			Size:             "1Gi",
		}}
	default:
		if err := os.MkdirAll(filepath.Join(root, archive.ManifestsDirName), 0o755); err != nil {
			t.Fatalf("create manifests directory: %v", err)
		}

		if err := os.WriteFile(
			filepath.Join(root, archive.ManifestsDirName, "configmap_root.yaml"),
			[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: root\n"),
			0o600,
		); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
	}

	finalizeLocalNode(t, root, sy)

	switch mutation {
	case localArchiveValid:
	case localArchiveCorruptPayload:
		if err := os.WriteFile(filepath.Join(root, archive.DataBlockBase), []byte("corrupt payload"), 0o600); err != nil {
			t.Fatalf("corrupt payload: %v", err)
		}
	case localArchiveCorruptManifest:
		if err := os.WriteFile(
			filepath.Join(root, archive.ManifestsDirName, "configmap_root.yaml"),
			[]byte("corrupt manifest"),
			0o600,
		); err != nil {
			t.Fatalf("corrupt manifest: %v", err)
		}
	case localArchiveCorruptMetadata:
		rewriteLocalSnapshotYAML(t, root, func(fields map[string]interface{}) {
			fields["name"] = "tampered"
		})
	case localArchiveMissingVersion:
		rewriteLocalSnapshotYAML(t, root, func(fields map[string]interface{}) {
			delete(fields, "formatVersion")
			delete(fields, "metadataChecksum")
		})
	case localArchiveExplicitVersion0:
		rewriteLocalSnapshotYAML(t, root, func(fields map[string]interface{}) {
			fields["formatVersion"] = 0
			delete(fields, "metadataChecksum")
		})
	default:
		t.Fatalf("unknown archive mutation %q", mutation)
	}

	return root
}

func finalizeLocalNode(t *testing.T, dir string, sy archive.SnapshotYAML) {
	t.Helper()

	if err := os.MkdirAll(filepath.Join(dir, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("create manifests directory: %v", err)
	}

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("compute node checksum: %v", err)
	}

	sy.Checksum = checksum
	writeNodeYAML(t, dir, sy)
}

func rewriteLocalSnapshotYAML(t *testing.T, dir string, rewrite func(map[string]interface{})) {
	t.Helper()

	path := filepath.Join(dir, archive.SnapshotYAMLName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot.yaml: %v", err)
	}

	var fields map[string]interface{}
	if err := sigsyaml.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal snapshot.yaml: %v", err)
	}

	rewrite(fields)

	data, err = sigsyaml.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal snapshot.yaml: %v", err)
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("rewrite snapshot.yaml: %v", err)
	}
}
