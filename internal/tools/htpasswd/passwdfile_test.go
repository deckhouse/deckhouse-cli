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

package htpasswd

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// These unit tests exercise the password-FILE handling in passwdfile.go
// (loadPasswdFile / newPasswdFile / get / upsert / remove / save) and pin down
// its behavior against real Apache htpasswd. Identifiers are prefixed `pf_` to
// avoid collisions with the other _test.go files in package htpasswd.

// pf_write creates a file with exact permission bits (chmod after write so the
// umask cannot mask the mode we assert on) and returns its path.
func pf_write(t *testing.T, dir, name, content string, mode os.FileMode) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), mode))
	require.NoError(t, os.Chmod(path, mode))

	return path
}

// pf_read returns a file's contents as a string.
func pf_read(t *testing.T, path string) string {
	t.Helper()

	b, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(b)
}

// Test_pf_LineUser pins lineUser = substring before the first ':'. Note the two
// edge cases that matter for the empty-username collision below: a blank line
// has user "" and a comment keeps its whole text as the "user".
func Test_pf_LineUser(t *testing.T) {
	cases := []struct{ line, want string }{
		{"alice:$apr1$x", "alice"},
		{"", ""},                   // blank line -> user ""
		{"# comment", "# comment"}, // comment -> whole line
		{"nocolon", "nocolon"},
		{":hashonly", ""},
		{"a:b:c", "a"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, lineUser(c.line), "lineUser(%q)", c.line)
	}
}

// Test_pf_UpsertPreservesOtherLines is the core parity case: updating one user
// leaves every other line — users, a comment, a blank line — byte-for-byte in
// place, exactly like real htpasswd. upsert of an existing user returns true.
func Test_pf_UpsertPreservesOtherLines(t *testing.T) {
	dir := t.TempDir()
	path := pf_write(t, dir, "users", "# top comment\nalice:AAA\n\n# mid comment\nbob:BBB\n", 0o644)

	pf, err := loadPasswdFile(path)
	require.NoError(t, err)

	existed := pf.upsert("alice", "NEWALICE")
	require.True(t, existed, "alice already existed, upsert must report true")

	require.NoError(t, pf.save())

	require.Equal(t,
		"# top comment\nalice:NEWALICE\n\n# mid comment\nbob:BBB\n",
		pf_read(t, path),
		"only alice's line changes; comment, blank line, and bob stay in place",
	)
}

// Test_pf_UpsertAppendsNewUser: a brand-new user is appended after existing
// lines and upsert returns false (did not exist).
func Test_pf_UpsertAppendsNewUser(t *testing.T) {
	dir := t.TempDir()
	path := pf_write(t, dir, "users", "alice:AAA\n", 0o644)

	pf, err := loadPasswdFile(path)
	require.NoError(t, err)

	existed := pf.upsert("bob", "BBB")
	require.False(t, existed, "bob is new, upsert must report false")

	require.NoError(t, pf.save())
	require.Equal(t, "alice:AAA\nbob:BBB\n", pf_read(t, path))
}

// Test_pf_GetFirstMatchAndMissing: get returns the first match's hash and true;
// a missing user yields ("", false).
func Test_pf_GetFirstMatchAndMissing(t *testing.T) {
	pf := &passwdFile{lines: []string{"alice:AAA", "bob:BBB"}}

	hash, ok := pf.get("bob")
	require.True(t, ok)
	require.Equal(t, "BBB", hash)

	hash, ok = pf.get("carol")
	require.False(t, ok)
	require.Equal(t, "", hash)
}

// Test_pf_RemoveReturnsFoundAndAbsent: remove reports true and drops the line
// when present, false and leaves the slice intact when absent.
func Test_pf_RemoveReturnsFoundAndAbsent(t *testing.T) {
	pf := &passwdFile{lines: []string{"alice:AAA", "bob:BBB"}}

	require.True(t, pf.remove("alice"))
	require.Equal(t, []string{"bob:BBB"}, pf.lines)

	require.False(t, pf.remove("ghost"))
	require.Equal(t, []string{"bob:BBB"}, pf.lines, "a no-op remove must not disturb the file")
}

// Test_pf_DuplicateUser_RemoveDropsAll documents that remove deletes EVERY
// matching line. This MATCHES real htpasswd -D on a duplicate-user file.
func Test_pf_DuplicateUser_RemoveDropsAll(t *testing.T) {
	pf := &passwdFile{lines: []string{"alice:AAA", "bob:BBB", "alice:CCC"}}

	require.True(t, pf.remove("alice"))
	require.Equal(t, []string{"bob:BBB"}, pf.lines, "both alice entries removed, like real htpasswd -D")
}

// Test_pf_DuplicateUser_UpsertReplacesOnlyFirst documents the ASYMMETRY /
// PARITY-GAP: upsert replaces only the FIRST matching line and leaves later
// duplicates stale. Real htpasswd rewrites EVERY matching line, so after a
// password rotation the second (old-hash) line would linger here and still be
// honored by other consumers (Apache/nginx) of the file.
func Test_pf_DuplicateUser_UpsertReplacesOnlyFirst(t *testing.T) {
	pf := &passwdFile{lines: []string{"alice:AAA", "bob:BBB", "alice:CCC"}}

	existed := pf.upsert("alice", "UPDATED")
	require.True(t, existed)

	require.Equal(t,
		[]string{"alice:UPDATED", "bob:BBB", "alice:CCC"},
		pf.lines,
		"DIVERGENCE from htpasswd: only the first alice line is updated; alice:CCC stays stale",
	)

	// get still returns the first (now-updated) hash, so the stale line is shadowed here.
	hash, ok := pf.get("alice")
	require.True(t, ok)
	require.Equal(t, "UPDATED", hash)
}

// Test_pf_EmptyUsernameCollidesWithBlankLine documents a BUG: because a blank
// line's user is "", an empty-username get/upsert/remove collides with blank
// lines. Real htpasswd instead APPENDS a ":hash" line and preserves the blank
// line. validateUsername("") is allowed, so this is reachable from the CLI file
// flow (e.g. `d8 tools htpasswd -b -p file "" pw`).
func Test_pf_EmptyUsernameCollidesWithBlankLine(t *testing.T) {
	dir := t.TempDir()
	// Leading blank line, a comment, then a real user.
	path := pf_write(t, dir, "users", "\n# comment\nalice:AAA\n", 0o644)

	pf, err := loadPasswdFile(path)
	require.NoError(t, err)
	require.Equal(t, []string{"", "# comment", "alice:AAA"}, pf.lines)

	// get("") matches the blank line and falsely reports the empty user exists.
	hash, ok := pf.get("")
	require.True(t, ok, "BUG: get(\"\") matches a blank line")
	require.Equal(t, "", hash)

	// upsert("") OVERWRITES the blank line instead of appending, and reports
	// existed=true (so the CLI prints "Updating" rather than "Adding").
	existed := pf.upsert("", "ZZZ")
	require.True(t, existed, "BUG: upsert(\"\") reports the empty user pre-existed")
	require.Equal(t,
		[]string{":ZZZ", "# comment", "alice:AAA"},
		pf.lines,
		"BUG: the blank line is clobbered; real htpasswd would keep it and append :ZZZ",
	)

	// remove("") drops all blank lines.
	pf2, err := loadPasswdFile(path)
	require.NoError(t, err)
	require.True(t, pf2.remove(""), "BUG: remove(\"\") deletes blank lines")
	require.Equal(t, []string{"# comment", "alice:AAA"}, pf2.lines)
}

// Test_pf_PermissionBitsPreservedOnUpdate: loadPasswdFile captures the file's
// mode via os.Stat and save re-applies it, so an update keeps the original
// permission bits (0o600 here) even though save writes a brand-new inode.
func Test_pf_PermissionBitsPreservedOnUpdate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission bits")
	}

	dir := t.TempDir()
	path := pf_write(t, dir, "users", "alice:AAA\n", 0o600)

	pf, err := loadPasswdFile(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), pf.mode)

	pf.upsert("bob", "BBB")
	require.NoError(t, pf.save())

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "update must preserve the file's mode")
}

// Test_pf_NewFile_OverwritesContentAndResetsMode covers the '-c' flow:
// newPasswdFile does not read the existing file, so save OVERWRITES its content
// and resets the mode to the hardcoded default 0o644 (Chmod is exact — it
// ignores umask and the file's previous 0o600), regardless of what was there.
func Test_pf_NewFile_OverwritesContentAndResetsMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission bits")
	}

	dir := t.TempDir()
	path := pf_write(t, dir, "users", "old:OLD\nkeep:KEEP\n", 0o600)

	pf := newPasswdFile(path)
	pf.upsert("fresh", "NEW")
	require.NoError(t, pf.save())

	require.Equal(t, "fresh:NEW\n", pf_read(t, path), "-c discards all prior content")

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm(),
		"-c resets mode to the hardcoded 0o644 default (ignores umask and the prior 0o600)")
}

// Test_pf_LoadCRLF_RewrittenAsLF: bufio.Scanner strips \r, so a CRLF input file
// loads with clean hashes and save rewrites it LF-only. Real htpasswd keeps the
// original \r\n bytes on untouched lines; the LF normalization here is an
// acceptable (arguably safer) divergence for htpasswd files.
func Test_pf_LoadCRLF_RewrittenAsLF(t *testing.T) {
	dir := t.TempDir()
	path := pf_write(t, dir, "users", "alice:AAA\r\nbob:BBB\r\n", 0o644)

	pf, err := loadPasswdFile(path)
	require.NoError(t, err)

	// The trailing \r is stripped, so the parsed hash is clean.
	hash, ok := pf.get("bob")
	require.True(t, ok)
	require.Equal(t, "BBB", hash, "carriage return must not contaminate the hash field")

	pf.upsert("carol", "CCC")
	require.NoError(t, pf.save())

	out := pf_read(t, path)
	require.NotContains(t, out, "\r", "save normalizes line endings to LF")
	require.Equal(t, "alice:AAA\nbob:BBB\ncarol:CCC\n", out)
}

// Test_pf_Save_AddsTrailingNewline: a file with no trailing newline is rewritten
// with exactly one '\n' per line. Real htpasswd instead appends the new record
// directly onto the last (newline-less) line, corrupting it into
// "alice:AAAbob:BBB"; save's per-line rendering is the safer behavior.
func Test_pf_Save_AddsTrailingNewline(t *testing.T) {
	dir := t.TempDir()
	path := pf_write(t, dir, "users", "alice:AAA", 0o644) // no trailing newline

	pf, err := loadPasswdFile(path)
	require.NoError(t, err)

	pf.upsert("bob", "BBB")
	require.NoError(t, pf.save())

	require.Equal(t, "alice:AAA\nbob:BBB\n", pf_read(t, path),
		"save renders each entry on its own newline-terminated line")
}

// Test_pf_Remove_LastUserLeavesEmptyFile: deleting the only user leaves a
// present, 0-byte file rather than removing it — this MATCHES real htpasswd -D.
func Test_pf_Remove_LastUserLeavesEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := pf_write(t, dir, "users", "only:AAA\n", 0o644)

	pf, err := loadPasswdFile(path)
	require.NoError(t, err)

	require.True(t, pf.remove("only"))
	require.NoError(t, pf.save())

	info, err := os.Stat(path)
	require.NoError(t, err, "file must still exist")
	require.Equal(t, int64(0), info.Size(), "last-user deletion leaves a 0-byte file, like htpasswd")
}

// Test_pf_Load_MissingFileReturnsOSError: loadPasswdFile surfaces the raw os
// error so callers can detect not-exist and print a friendly message.
func Test_pf_Load_MissingFileReturnsOSError(t *testing.T) {
	_, err := loadPasswdFile(filepath.Join(t.TempDir(), "does-not-exist"))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err), "want an os not-exist error, got %v", err)
}

// Test_pf_Save_RequiresWritableDirectory documents a PARITY-GAP of the
// temp+rename design: save uses os.CreateTemp in the target's directory, so it
// fails when the DIRECTORY is not writable — even though the target file itself
// is writable. Real htpasswd rewrites the file in place and only needs the file
// writable. Skipped as root (root bypasses directory permissions) and on
// Windows (mode bits do not gate writes the same way).
func Test_pf_Save_RequiresWritableDirectory(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("directory permission gating is unix/non-root only")
	}

	dir := t.TempDir()
	path := pf_write(t, dir, "users", "alice:AAA\n", 0o644)

	pf, err := loadPasswdFile(path)
	require.NoError(t, err)
	pf.upsert("alice", "NEW")

	require.NoError(t, os.Chmod(dir, 0o555))    // read+exec, not writable
	defer func() { _ = os.Chmod(dir, 0o755) }() // restore so t.TempDir cleanup works

	err = pf.save()
	require.Error(t, err, "save cannot create its temp file in a non-writable directory")
	require.Contains(t, err.Error(), "creating temp file")
}
