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

package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// run executes a fresh command with the given args and optional stdin, and
// returns stdout+stderr combined.
func run(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()

	if stdin != "" {
		restore := withStdin(t, stdin)
		defer restore()
	}

	cmd := NewCommand()

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)

	err := cmd.Execute()

	return out.String(), err
}

func Test_Stdout_BareHashAndUserLine(t *testing.T) {
	// Bare hash (no username) — the form 'd8 iam user --password-hash' consumes.
	out, err := run(t, "Test12345!", "-n", "-i", "-C", "4")
	require.NoError(t, err)

	bare := strings.TrimSpace(out)
	require.True(t, strings.HasPrefix(bare, "$2y$"), "want bcrypt hash, got %q", bare)
	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(bare), []byte("Test12345!")))

	// With a username — the htpasswd "user:hash" line.
	out, err = run(t, "Test12345!", "-n", "-i", "-C", "4", "admin")
	require.NoError(t, err)

	user, hash, found := strings.Cut(strings.TrimSpace(out), ":")
	require.True(t, found)
	require.Equal(t, "admin", user)
	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(hash), []byte("Test12345!")))
}

// Test_Stdout_HtpasswdDropInRecipe locks in the README recipe: the exact Apache
// htpasswd invocation must work verbatim with 'd8 tools htpasswd' substituted
// for 'htpasswd', including the bundled flags (-BinC) and the empty-username
// ":hash" output that 'cut -d: -f2' relies on.
//
//	echo -n 'Test12345!' | d8 tools htpasswd -BinC 10 "" | cut -d: -f2 | tr -d '\n'
func Test_Stdout_HtpasswdDropInRecipe(t *testing.T) {
	out, err := run(t, "Test12345!", "-BinC", "10", "")
	require.NoError(t, err)

	// Like real htpasswd with an empty username, the line begins with ':'.
	line := strings.TrimRight(out, "\n")
	require.True(t, strings.HasPrefix(line, ":"), "want leading colon like htpasswd, got %q", line)

	// Emulate 'cut -d: -f2 | tr -d \n'.
	_, hash, _ := strings.Cut(line, ":")
	require.True(t, strings.HasPrefix(hash, "$2y$"), "want bcrypt $2y$ hash, got %q", hash)

	cost, err := bcrypt.Cost([]byte(hash))
	require.NoError(t, err)
	require.Equal(t, 10, cost, "-C 10 should yield cost 10")

	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(hash), []byte("Test12345!")))
}

func Test_File_CreateVerifyDeleteLifecycle(t *testing.T) {
	file := filepath.Join(t.TempDir(), "users.htpasswd")

	// Create the file and add alice (batch mode, low cost for speed).
	_, err := run(t, "", "-c", "-b", "-C", "4", file, "alice", "AlicePass1")
	require.NoError(t, err)

	// Add a second user, preserving the first.
	_, err = run(t, "", "-b", "-C", "4", file, "bob", "BobPass1")
	require.NoError(t, err)

	content, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Contains(t, string(content), "alice:$2y$")
	require.Contains(t, string(content), "bob:$2y$")

	// Verify correct and incorrect passwords.
	_, err = run(t, "", "-bv", file, "alice", "AlicePass1")
	require.NoError(t, err)

	_, err = run(t, "", "-bv", file, "alice", "WrongPass")
	require.Error(t, err)

	// Delete bob; alice remains.
	_, err = run(t, "", "-D", file, "bob")
	require.NoError(t, err)

	content, err = os.ReadFile(file)
	require.NoError(t, err)
	require.Contains(t, string(content), "alice:")
	require.NotContains(t, string(content), "bob:")

	// Deleting a missing user is an error.
	_, err = run(t, "", "-D", file, "ghost")
	require.Error(t, err)
}

func Test_File_AlgorithmsAndConflicts(t *testing.T) {
	dir := t.TempDir()

	// apr1 (-m) and SHA-512 (-5) entries land with the expected prefixes.
	fileM := filepath.Join(dir, "m.htpasswd")
	_, err := run(t, "", "-c", "-b", "-m", fileM, "u", "pw123456")
	require.NoError(t, err)
	content, _ := os.ReadFile(fileM)
	require.Contains(t, string(content), "u:$apr1$")

	file5 := filepath.Join(dir, "s5.htpasswd")
	_, err = run(t, "", "-c", "-b", "-5", "-r", "1000", file5, "u", "pw123456")
	require.NoError(t, err)
	content, _ = os.ReadFile(file5)
	require.Contains(t, string(content), "u:$6$rounds=1000$")

	// Verify one of them round-trips through the CLI.
	_, err = run(t, "", "-bv", file5, "u", "pw123456")
	require.NoError(t, err)

	// Conflicting flags are rejected.
	_, err = run(t, "", "-c", "-n", "x")
	require.Error(t, err)

	_, err = run(t, "", "-B", "-m", "-n", "x")
	require.Error(t, err)

	// -C without -B is rejected.
	_, err = run(t, "", "-m", "-C", "8", "-n", "x")
	require.Error(t, err)
}

// withStdin swaps os.Stdin for a pipe preloaded with content and returns a
// restore func.
func withStdin(t *testing.T, content string) func() {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	_, err = w.WriteString(content + "\n")
	require.NoError(t, err)
	require.NoError(t, w.Close())

	orig := os.Stdin
	os.Stdin = r

	return func() { os.Stdin = orig }
}
