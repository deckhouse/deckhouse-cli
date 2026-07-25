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
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/local"
)

func TestLocalDescribeVerifiesIntegrityBeforeRendering(t *testing.T) {
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

			cmd := local.NewDescribeCommand(discardLog())
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

			if !strings.Contains(output.String(), root) {
				t.Fatalf("output = %q, want archive root %q", output.String(), root)
			}
		})
	}
}

func TestLocalDescribeLegacyCompatibilityIsExplicitAndWarns(t *testing.T) {
	root := buildLocalIntegrityArchive(t, localArchiveMissingVersion)
	var output bytes.Buffer
	var logs bytes.Buffer

	cmd := local.NewDescribeCommand(slog.New(slog.NewTextHandler(&logs, nil)))
	cmd.SetOut(&output)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--allow-unauthenticated-legacy", root})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute(): %v", err)
	}

	if !strings.Contains(output.String(), root) {
		t.Fatalf("legacy output = %q, want archive root %q", output.String(), root)
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

func TestLocalDescribeHelpDocumentsDefaultVerification(t *testing.T) {
	cmd := local.NewDescribeCommand(discardLog())

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
