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

package download

import (
	"log/slog"
	"testing"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

func TestParseChunkSize_EmptyReturnsZero(t *testing.T) {
	t.Helper()

	n, err := parseChunkSize("")
	if err != nil {
		t.Fatalf("unexpected error for empty string: %v", err)
	}

	if n != 0 {
		t.Fatalf("expected 0 for empty input, got %d", n)
	}
}

func TestParseChunkSize(t *testing.T) {
	t.Helper()

	cases := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{"256Mi", 256 * 1024 * 1024, false},
		{"256MiB", 256 * 1024 * 1024, false},
		{"1Gi", 1 * 1024 * 1024 * 1024, false},
		{"1GiB", 1 * 1024 * 1024 * 1024, false},
		{"512Mi", 512 * 1024 * 1024, false},
		{"128M", 128 * 1000 * 1000, false},
		{"128MB", 128 * 1000 * 1000, false},
		{"1G", 1 * 1000 * 1000 * 1000, false},
		// too small: below DefaultChunkSize/16
		{"1Ki", 0, true},
		// zero
		{"0Mi", 0, true},
		// negative
		{"-1Mi", 0, true},
		// bad string
		{"abc", 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseChunkSize(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("input %q: expected error, got nil (result=%d)", tc.input, got)
				}

				return
			}

			if err != nil {
				t.Fatalf("input %q: unexpected error: %v", tc.input, err)
			}

			if got != tc.want {
				t.Fatalf("input %q: got %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseChunkSize_DefaultMinimum(t *testing.T) {
	t.Helper()

	// Exactly DefaultChunkSize (256 MiB) should parse fine.
	const defaultChunkSize = 256 * 1024 * 1024 // mirrors volume.DefaultChunkSize
	s := "256Mi"

	n, err := parseChunkSize(s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n != defaultChunkSize {
		t.Fatalf("got %d, want %d", n, defaultChunkSize)
	}
}

func TestNewCommand_Defaults(t *testing.T) {
	t.Helper()

	log := slog.Default()
	cmd := NewCommand(log)

	wantUse := cmdUse + " [flags] <snapshot>"
	if cmd.Use != wantUse {
		t.Fatalf("unexpected Use: got %q, want %q", cmd.Use, wantUse)
	}

	if !cmd.SilenceUsage {
		t.Fatal("SilenceUsage should be true")
	}

	if !cmd.SilenceErrors {
		t.Fatal("SilenceErrors should be true")
	}

	// Check default flag values.
	ttl, err := cmd.Flags().GetString(flagTTL)
	if err != nil {
		t.Fatalf("getting ttl flag: %v", err)
	}

	if ttl != "2h" {
		t.Fatalf("default ttl: got %q, want %q", ttl, "2h")
	}

	workers, err := cmd.Flags().GetInt(flagWorkers)
	if err != nil {
		t.Fatalf("getting workers flag: %v", err)
	}

	if workers != 4 {
		t.Fatalf("default workers: got %d, want 4", workers)
	}

	perVol, err := cmd.Flags().GetInt(flagPerVolumeConcurrency)
	if err != nil {
		t.Fatalf("getting per-volume-concurrency flag: %v", err)
	}

	if perVol != 4 {
		t.Fatalf("default per-volume-concurrency: got %d, want 4", perVol)
	}

	chunkSize, err := cmd.Flags().GetString(flagChunkSize)
	if err != nil {
		t.Fatalf("getting chunk-size flag: %v", err)
	}

	if chunkSize != "" {
		t.Fatalf("default chunk-size: got %q, want empty", chunkSize)
	}
}

func TestNewCommand_NamespaceFlagDefault(t *testing.T) {
	t.Helper()

	log := slog.Default()
	cmd := NewCommand(log)

	ns, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		t.Fatalf("getting namespace flag: %v", err)
	}

	if ns != dataio.Namespace {
		t.Fatalf("default namespace: got %q, want %q", ns, dataio.Namespace)
	}
}

func TestNewCommand_RequiresOneArg(t *testing.T) {
	t.Helper()

	log := slog.Default()
	cmd := NewCommand(log)

	// Zero args: must error.
	if err := cmd.Args(cmd, []string{}); err == nil {
		t.Fatal("expected error with zero positional args, got nil")
	}

	// One arg (snapshot name only): must succeed.
	if err := cmd.Args(cmd, []string{"my-snap"}); err != nil {
		t.Fatalf("expected no error with one positional arg, got: %v", err)
	}

	// Two args: must error (namespace is now a flag, not a positional arg).
	if err := cmd.Args(cmd, []string{"ns", "snap"}); err == nil {
		t.Fatal("expected error with two positional args, got nil")
	}
}

func TestNewCommand_CompressionFlagDefaults(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	codec, err := cmd.Flags().GetString(flagVolumeCompression)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagVolumeCompression, err)
	}

	if codec != "zstd" {
		t.Fatalf("default codec: got %q, want %q", codec, "zstd")
	}

	level, err := cmd.Flags().GetInt(flagVolumeCompressionLevel)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagVolumeCompressionLevel, err)
	}

	if level != 0 {
		t.Fatalf("default compression level: got %d, want 0", level)
	}
}

func TestParseCompressionCodec_ValidNames(t *testing.T) {
	t.Helper()

	for _, name := range []string{"zstd", "lz4", "gzip", "none"} {
		t.Run(name, func(t *testing.T) {
			c, err := compress.New(name, 0)
			if err != nil {
				t.Fatalf("compress.New(%q, 0) unexpected error: %v", name, err)
			}

			if c == nil {
				t.Fatalf("compress.New(%q, 0) returned nil codec", name)
			}
		})
	}
}

func TestParseCompressionCodec_UnknownName(t *testing.T) {
	t.Helper()

	_, err := compress.New("bogus-codec", 0)
	if err == nil {
		t.Fatal("expected error for unknown codec name, got nil")
	}
}
