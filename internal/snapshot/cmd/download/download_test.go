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
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		// Golden values: resource.ParseQuantity must yield the same byte counts
		// the old hand-rolled parser produced for these spellings.
		{"256Mi", 256 * 1024 * 1024, false},
		// at max: exactly maxChunkSize (4x DefaultChunkSize == 1 GiB)
		{"1Gi", 1 * 1024 * 1024 * 1024, false},
		{"512Mi", 512 * 1024 * 1024, false},
		{"128M", 128 * 1000 * 1000, false},
		{"1G", 1 * 1000 * 1000 * 1000, false},
		// Deliberately dropped legacy spellings: "MiB"/"GiB"/"MB" and uppercase
		// "K" are NOT resource.Quantity suffixes and now error (reflected in the
		// flag help). Previously these were accepted by the hand-rolled parser.
		{"256MiB", 0, true},
		{"1GiB", 0, true},
		{"128MB", 0, true},
		{"1K", 0, true},
		// too small: below DefaultChunkSize/16
		{"1Ki", 0, true},
		// zero
		{"0Mi", 0, true},
		// negative
		{"-1Mi", 0, true},
		// bad string
		{"abc", 0, true},
		// just above maxChunkSize
		{"1025Mi", 0, true},
		// well above maxChunkSize
		{"4Gi", 0, true},
		// Trailing/embedded garbage that the old fmt.Sscanf("%d") parser
		// silently truncated to a different size — must now be rejected.
		{"12x3Mi", 0, true},
		{"12 3Mi", 0, true},
		{"12x3", 0, true},
		{"256 Mi", 0, true},
		{"Mi", 0, true},
		{"--5Mi", 0, true},
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

func TestParseChunkSize_Maximum(t *testing.T) {
	t.Helper()

	// Exactly maxChunkSize (1 GiB) should parse fine.
	n, err := parseChunkSize("1Gi")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n != maxChunkSize {
		t.Fatalf("got %d, want %d", n, maxChunkSize)
	}

	// Anything above maxChunkSize must be rejected.
	_, err = parseChunkSize("1025Mi")
	if err == nil {
		t.Fatal("expected error for chunk size above maximum, got nil")
	}
}

func TestNewCommand_Defaults(t *testing.T) {
	t.Helper()

	log := slog.Default()
	cmd := NewCommand(context.Background(), log)

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
	cmd := NewCommand(context.Background(), log)

	ns, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		t.Fatalf("getting namespace flag: %v", err)
	}

	if ns != "" {
		t.Fatalf("default namespace: got %q, want empty string (namespace is required)", ns)
	}
}

func TestRun_RequiresNamespace(t *testing.T) {
	t.Helper()

	cmd := NewCommand(context.Background(), slog.Default())

	err := Run(context.Background(), slog.Default(), cmd, []string{"my-snap"})
	if err == nil {
		t.Fatal("expected error when namespace is empty, got nil")
	}

	if !strings.Contains(err.Error(), flagNamespace) {
		t.Fatalf("expected error to mention %q, got: %v", flagNamespace, err)
	}
}

func TestNewCommand_RequiresOneArg(t *testing.T) {
	t.Helper()

	log := slog.Default()
	cmd := NewCommand(context.Background(), log)

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

// TestNewCommand_CompressionFlagsVisible asserts that --volume-compression and
// --volume-compression-level are now part of the user-facing contract: both
// flags are registered and NOT hidden from `-h`/completion. This replaces the
// former TestNewCommand_CompressionFlagsHidden, which asserted the (now
// reversed) hidden state from when compression selection was withdrawn.
func TestNewCommand_CompressionFlagsVisible(t *testing.T) {
	t.Helper()

	cmd := NewCommand(context.Background(), slog.Default())

	compressionFlag := cmd.Flags().Lookup(flagVolumeCompression)
	if compressionFlag == nil {
		t.Fatalf("flag %s: not registered", flagVolumeCompression)
	}

	if compressionFlag.Hidden {
		t.Fatalf("flag %s: got Hidden=%v, want false", flagVolumeCompression, compressionFlag.Hidden)
	}

	levelFlag := cmd.Flags().Lookup(flagVolumeCompressionLevel)
	if levelFlag == nil {
		t.Fatalf("flag %s: not registered", flagVolumeCompressionLevel)
	}

	if levelFlag.Hidden {
		t.Fatalf("flag %s: got Hidden=%v, want false", flagVolumeCompressionLevel, levelFlag.Hidden)
	}
}

// TestNewCommand_CompressionFlagUsage_ListsOnlyUserSelectableCodecs asserts the
// --volume-compression help text advertises exactly the current user-selectable
// allow-list (compress.UserSelectableNames(): "zstd" and "none") and does NOT
// mention "gzip"/"lz4" — those codecs stay registered for decoding but are
// withheld from the user-facing choice (see compress.userSelectableNames' doc
// comment for why).
func TestNewCommand_CompressionFlagUsage_ListsOnlyUserSelectableCodecs(t *testing.T) {
	t.Helper()

	cmd := NewCommand(context.Background(), slog.Default())

	compressionFlag := cmd.Flags().Lookup(flagVolumeCompression)
	if compressionFlag == nil {
		t.Fatalf("flag %s: not registered", flagVolumeCompression)
	}

	usage := compressionFlag.Usage

	if !strings.Contains(usage, "zstd") {
		t.Fatalf("usage does not mention %q: %q", "zstd", usage)
	}

	if !strings.Contains(usage, "none") {
		t.Fatalf("usage does not mention %q: %q", "none", usage)
	}

	if strings.Contains(usage, "gzip") {
		t.Fatalf("usage still mentions withheld codec %q: %q", "gzip", usage)
	}

	if strings.Contains(usage, "lz4") {
		t.Fatalf("usage still mentions withheld codec %q: %q", "lz4", usage)
	}
}

// TestValidateVolumeCompression asserts that only the user-selectable codec
// names (zstd, none) are accepted at flag-validation time, while codecs that
// remain registered for decoding but are withheld from user choice (gzip,
// lz4) and outright unknown names are rejected with a clear error before any
// data transfer begins.
func TestValidateVolumeCompression(t *testing.T) {
	t.Helper()

	cases := []struct {
		name    string
		wantErr bool
	}{
		{"zstd", false},
		{"none", false},
		{"lz4", true},
		{"gzip", true},
		{"bogus-codec", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			codec, err := validateVolumeCompression(tc.name, 0)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("validateVolumeCompression(%q, 0): expected error, got nil", tc.name)
				}

				if !strings.Contains(err.Error(), tc.name) {
					t.Fatalf("validateVolumeCompression(%q, 0): error does not name the rejected codec: %v", tc.name, err)
				}

				return
			}

			if err != nil {
				t.Fatalf("validateVolumeCompression(%q, 0): unexpected error: %v", tc.name, err)
			}

			if codec == nil {
				t.Fatalf("validateVolumeCompression(%q, 0): returned nil codec", tc.name)
			}

			if codec.Name() != tc.name {
				t.Fatalf("validateVolumeCompression(%q, 0): codec.Name() = %q", tc.name, codec.Name())
			}
		})
	}
}

// TestNewCommand_NodeExample_ShowsOriginalIdentityForm asserts the --node Example
// text leads with the ORIGINAL captured object's Kind/Name form (e.g.
// "DemoVirtualDisk/bk-disk-a") rather than the generated snapshot CR name form,
// and that the CR-name form is still mentioned as accepted (back-compat) --
// backlog #20's decision that the original identity is now the preferred,
// no-less-valid, form.
func TestNewCommand_NodeExample_ShowsOriginalIdentityForm(t *testing.T) {
	t.Helper()

	cmd := NewCommand(context.Background(), slog.Default())

	if !strings.Contains(cmd.Example, "--node DemoVirtualDisk/bk-disk-a") {
		t.Fatalf("Example text does not lead with the original-identity --node form:\n%s", cmd.Example)
	}

	if !strings.Contains(cmd.Example, "DemoVirtualDiskSnapshot/nss-child-abc123") {
		t.Fatalf("Example text dropped the back-compat mention of the CR-name --node form:\n%s", cmd.Example)
	}

	nodeFlag := cmd.Flags().Lookup(flagNode)
	if nodeFlag == nil {
		t.Fatal("flag --node: not registered")
	}

	if !strings.Contains(nodeFlag.Usage, "DemoVirtualDisk/bk-disk-a") {
		t.Fatalf("--node usage does not lead with the original-identity form: %q", nodeFlag.Usage)
	}

	if !strings.Contains(nodeFlag.Usage, "still accepted") {
		t.Fatalf("--node usage does not note the CR-name form is still accepted: %q", nodeFlag.Usage)
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

func TestParseNodeFlag(t *testing.T) {
	t.Helper()

	cases := []struct {
		input    string
		wantKind string
		wantName string
		wantErr  bool
	}{
		// empty → full tree
		{"", "", "", false},
		// valid simple kind/name
		{"Snapshot/my-snap", "Snapshot", "my-snap", false},
		// domain kind with UUID-style name
		{"DemoVirtualDiskSnapshot/nss-child-abc123", "DemoVirtualDiskSnapshot", "nss-child-abc123", false},
		// VolumeSnapshot leaf
		{"VolumeSnapshot/demo-pvc", "VolumeSnapshot", "demo-pvc", false},
		// no slash → error
		{"NoSlashHere", "", "", true},
		// leading slash (empty kind) → error
		{"/name", "", "", true},
		// trailing slash (empty name) → error
		{"Kind/", "", "", true},
		// two slashes → error
		{"Kind/a/b", "", "", true},
		// just a slash → error (empty kind)
		{"/", "", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			gotKind, gotName, err := parseNodeFlag(tc.input)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("input %q: expected error, got nil (kind=%q name=%q)", tc.input, gotKind, gotName)
				}

				return
			}

			if err != nil {
				t.Fatalf("input %q: unexpected error: %v", tc.input, err)
			}

			if gotKind != tc.wantKind {
				t.Fatalf("input %q: got kind=%q, want %q", tc.input, gotKind, tc.wantKind)
			}

			if gotName != tc.wantName {
				t.Fatalf("input %q: got name=%q, want %q", tc.input, gotName, tc.wantName)
			}
		})
	}
}

func TestNewCommand_NodeFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(context.Background(), slog.Default())

	node, err := cmd.Flags().GetString(flagNode)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagNode, err)
	}

	if node != "" {
		t.Fatalf("default --node: got %q, want empty string (full-tree download by default)", node)
	}
}

func TestRun_NodeFlag_InvalidFormat(t *testing.T) {
	t.Helper()

	cmd := NewCommand(context.Background(), slog.Default())

	if err := cmd.Flags().Set(flagNamespace, "test-ns"); err != nil {
		t.Fatalf("setting namespace flag: %v", err)
	}

	if err := cmd.Flags().Set(flagOutput, t.TempDir()); err != nil {
		t.Fatalf("setting output flag: %v", err)
	}

	if err := cmd.Flags().Set(flagNode, "NoSlashHere"); err != nil {
		t.Fatalf("setting node flag: %v", err)
	}

	err := Run(context.Background(), slog.Default(), cmd, []string{"my-snap"})
	if err == nil {
		t.Fatal("expected error for invalid --node flag, got nil")
	}

	if !strings.Contains(err.Error(), flagNode) {
		t.Fatalf("expected error to mention %q, got: %v", flagNode, err)
	}
}

func TestAcquireOutputLock_Contention(t *testing.T) {
	t.Helper()

	dir := t.TempDir()

	first, err := acquireOutputLock(dir)
	if err != nil {
		t.Fatalf("first acquire: unexpected error: %v", err)
	}

	defer func() { _ = first.Unlock() }()

	_, err = acquireOutputLock(dir)
	if err == nil {
		t.Fatal("expected contention error on second acquire, got nil")
	}

	if !errors.Is(err, ErrOutputDirLocked) {
		t.Fatalf("expected ErrOutputDirLocked, got: %v", err)
	}

	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("expected error to name the directory %q, got: %v", dir, err)
	}
}

func TestAcquireOutputLock_ReacquireAfterRelease(t *testing.T) {
	t.Helper()

	dir := t.TempDir()

	first, err := acquireOutputLock(dir)
	if err != nil {
		t.Fatalf("first acquire: unexpected error: %v", err)
	}

	if err := first.Unlock(); err != nil {
		t.Fatalf("unlock: unexpected error: %v", err)
	}

	second, err := acquireOutputLock(dir)
	if err != nil {
		t.Fatalf("second acquire after release: unexpected error: %v", err)
	}

	defer func() { _ = second.Unlock() }()
}

// TestAcquireOutputLock_StaleLockFileIsHarmless documents and locks in the
// stale-lock policy from acquireOutputLock's doc comment: the lock file is a
// plain flock(2), which the OS releases automatically when the holding
// process exits for any reason (including a hard kill). A lock FILE left on
// disk with no live holder — simulated here by pre-creating the file without
// ever flock-ing it — must not block a fresh acquire.
func TestAcquireOutputLock_StaleLockFileIsHarmless(t *testing.T) {
	t.Helper()

	dir := t.TempDir()

	lockPath := filepath.Join(dir, downloadLockFileName)
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatalf("pre-creating stale lock file: %v", err)
	}

	fl, err := acquireOutputLock(dir)
	if err != nil {
		t.Fatalf("expected a pre-existing, unheld lock file to be reclaimed, got: %v", err)
	}

	defer func() { _ = fl.Unlock() }()
}

// TestRun_ReleasesLockOnCancelledContext verifies the lock acquired near the
// top of Run is released via defer even when the caller's context is already
// cancelled by the time Run returns. The cancelled ctx is threaded in through
// the Run parameter (Run derives its signal context from that parameter, not
// from cmd.Context()). It forces an early, ctx-independent error path (an
// invalid --node flag, validated before any cluster client is built) so the
// test stays deterministic and network-free while still exercising Run with a
// cancelled context end to end.
func TestRun_ReleasesLockOnCancelledContext(t *testing.T) {
	t.Helper()

	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := NewCommand(context.Background(), slog.Default())

	if err := cmd.Flags().Set(flagNamespace, "test-ns"); err != nil {
		t.Fatalf("setting namespace flag: %v", err)
	}

	if err := cmd.Flags().Set(flagOutput, dir); err != nil {
		t.Fatalf("setting output flag: %v", err)
	}

	if err := cmd.Flags().Set(flagNode, "NoSlashHere"); err != nil {
		t.Fatalf("setting node flag: %v", err)
	}

	if err := Run(ctx, slog.Default(), cmd, []string{"my-snap"}); err == nil {
		t.Fatal("expected error from invalid --node flag, got nil")
	}

	// The lock must have been released on the way out despite the cancelled
	// context: a fresh acquire on the same directory must succeed.
	fl, err := acquireOutputLock(dir)
	if err != nil {
		t.Fatalf("expected lock to be released after Run returned, got: %v", err)
	}

	defer func() { _ = fl.Unlock() }()
}

// TestRun_CancelsOnCancelledParentCtx pins the new §4 contract: Run roots its
// signal-cancellable context in the ctx PARAMETER (the caller-owned root
// context threaded through NewCommand), not in cmd.Context(). Here the command
// carries NO context of its own (cmd.Context() is nil, which under the old
// code would have fallen back to context.Background()); Run must instead use
// the already-cancelled parent ctx passed as its first argument. The flow is
// steered to a deterministic, ctx-independent early error (invalid --node)
// validated before any cluster client is built, so the test proves Run accepts
// and threads a cancelled parent ctx and returns promptly without hanging or
// relying on cmd.Context().
func TestRun_CancelsOnCancelledParentCtx(t *testing.T) {
	t.Helper()

	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := NewCommand(context.Background(), slog.Default())

	if err := cmd.Flags().Set(flagNamespace, "test-ns"); err != nil {
		t.Fatalf("setting namespace flag: %v", err)
	}

	if err := cmd.Flags().Set(flagOutput, dir); err != nil {
		t.Fatalf("setting output flag: %v", err)
	}

	if err := cmd.Flags().Set(flagNode, "NoSlashHere"); err != nil {
		t.Fatalf("setting node flag: %v", err)
	}

	if err := Run(ctx, slog.Default(), cmd, []string{"my-snap"}); err == nil {
		t.Fatal("expected error when running with a cancelled parent ctx, got nil")
	}
}
