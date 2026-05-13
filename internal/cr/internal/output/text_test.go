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

package output

import (
	"bytes"
	"io/fs"
	"math"
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imagefs"
)

func TestHumanSize(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		// EB scale: 1<<60 == 1024^6 - the largest unit reachable for int64.
		{1 << 60, "1.0 EB"},
		// Boundary: int64 max must not panic, must stay within "EB".
		// (loop cannot reach exp=6 because 1024^7 > MaxInt64).
		{math.MaxInt64, "8.0 EB"},
	}
	for _, tc := range cases {
		got := HumanSize(tc.in)
		if got != tc.want {
			t.Errorf("HumanSize(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWriteEntriesText_ShortFormat(t *testing.T) {
	entries := []imagefs.Entry{
		{Path: "etc", Type: imagefs.TypeDir, Mode: fs.ModeDir | 0o755},
		{Path: "etc/passwd", Type: imagefs.TypeFile, Size: 42, Mode: 0o644},
	}
	var buf bytes.Buffer
	if err := WriteEntriesText(&buf, entries, false); err != nil {
		t.Fatalf("WriteEntriesText: %v", err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), buf.String())
	}
	// Directory rendered with trailing slash.
	if lines[0] != "etc/" {
		t.Errorf("dir line = %q, want %q", lines[0], "etc/")
	}
	if lines[1] != "etc/passwd" {
		t.Errorf("file line = %q, want %q", lines[1], "etc/passwd")
	}
}

func TestWriteEntriesText_LongFormat(t *testing.T) {
	entries := []imagefs.Entry{
		{Path: "etc/passwd", Type: imagefs.TypeFile, Size: 1024, Mode: 0o644, ModeStr: "-rw-r--r--"},
	}
	var buf bytes.Buffer
	if err := WriteEntriesText(&buf, entries, true); err != nil {
		t.Fatalf("WriteEntriesText: %v", err)
	}
	out := buf.String()
	// Long format must include mode, size and path - exact column widths are
	// not part of the contract, only that all three fields are present.
	for _, want := range []string{"-rw-r--r--", "1.0 KB", "etc/passwd"} {
		if !strings.Contains(out, want) {
			t.Errorf("long output missing %q: %q", want, out)
		}
	}
}

func TestWriteEntriesText_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteEntriesText(&buf, nil, false); err != nil {
		t.Fatalf("WriteEntriesText: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("expected no output for empty input, got %q", buf.String())
	}
}
