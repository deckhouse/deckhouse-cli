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

package volume

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// writeEmptyFile creates a zero-byte file and returns its path.
func writeEmptyFile(t *testing.T) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatalf("write empty file: %v", err)
	}

	return p
}

// TestVerifyDecodedLength_EmptyInputZeroWant proves the defensive symmetry with
// the MergeBlockChunks zero-size short-circuit: an empty input with wantSize=0
// is a valid zero-length decode under EVERY codec extension, including ".gz"
// whose reader would otherwise reject an empty stream with EOF.
func TestVerifyDecodedLength_EmptyInputZeroWant(t *testing.T) {
	for _, ext := range []string{"", ".zst", ".gz", ".lz4"} {
		t.Run("ext="+ext, func(t *testing.T) {
			if err := verifyDecodedLength(writeEmptyFile(t), ext, 0); err != nil {
				t.Errorf("verifyDecodedLength(empty, %q, 0) = %v, want nil", ext, err)
			}
		})
	}
}

// TestVerifyDecodedLength_EmptyInputNonZeroWant proves the zero guard does NOT
// mask a real mismatch: an empty input with a non-zero wantSize still fails —
// either as a decode error (gzip cannot read an empty member) or as an
// ErrDecodedLengthMismatch (codecs that decode empty input to zero bytes).
func TestVerifyDecodedLength_EmptyInputNonZeroWant(t *testing.T) {
	tests := []struct {
		ext          string
		wantMismatch bool // true: expect ErrDecodedLengthMismatch; false: any decode error
	}{
		{ext: "", wantMismatch: true},
		{ext: ".zst", wantMismatch: true},
		{ext: ".lz4", wantMismatch: true},
		{ext: ".gz", wantMismatch: false},
	}

	for _, tc := range tests {
		t.Run("ext="+tc.ext, func(t *testing.T) {
			err := verifyDecodedLength(writeEmptyFile(t), tc.ext, 5)
			if err == nil {
				t.Fatalf("verifyDecodedLength(empty, %q, 5) = nil, want error", tc.ext)
			}

			if tc.wantMismatch && !errors.Is(err, ErrDecodedLengthMismatch) {
				t.Errorf("verifyDecodedLength(empty, %q, 5) = %v, want ErrDecodedLengthMismatch", tc.ext, err)
			}
		})
	}
}
