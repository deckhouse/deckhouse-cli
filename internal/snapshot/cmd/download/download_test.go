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
	"context"
	"log/slog"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
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

	// Exactly DefaultChunkSize should parse fine.
	def := int64(volume.DefaultChunkSize)
	s := "256Mi"

	n, err := parseChunkSize(s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n != def {
		t.Fatalf("got %d, want %d", n, def)
	}
}

func TestNewCommand_Defaults(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	log := slog.Default()
	cmd := NewCommand(ctx, log)

	if cmd.Use != cmdUse+" <namespace> <snapshot>" {
		t.Fatalf("unexpected Use: %q", cmd.Use)
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

func TestNewCommand_RequiresTwoArgs(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	log := slog.Default()
	cmd := NewCommand(ctx, log)

	err := cmd.Args(cmd, []string{"only-one"})
	if err == nil {
		t.Fatal("expected error with one positional arg, got nil")
	}

	err = cmd.Args(cmd, []string{"ns", "snap"})
	if err != nil {
		t.Fatalf("expected no error with two positional args, got: %v", err)
	}
}
