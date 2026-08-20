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
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// Test_cli_FlagBundling verifies pflag shorthand bundling — including the digit
// algorithm shorthands -2/-5 — parses and routes to the right algorithm. These
// bundle forms are accepted by real htpasswd too (except -2/-5, which are d8
// extensions); parsing them is the parity surface this locks in.
func Test_cli_FlagBundling(t *testing.T) {
	// -nbB: bcrypt via a bundle, password from the command line.
	out, err := run(t, "", "-nbB", "-C", "4", "u", "p")
	require.NoError(t, err)
	_, hash, found := strings.Cut(strings.TrimSpace(out), ":")
	require.True(t, found, "want user:hash line, got %q", out)
	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(hash), []byte("p")))

	// -nbm -> apr1.
	out, err = run(t, "", "-nbm", "u", "p")
	require.NoError(t, err)
	require.Contains(t, out, "u:$apr1$")

	// -nb2 / -nb5 -> SHA-256 / SHA-512 crypt (d8 extensions; digit shorthands
	// must still bundle). -r must be honoured only alongside these.
	out, err = run(t, "", "-nb2", "-r", "1000", "u", "p")
	require.NoError(t, err)
	require.Contains(t, out, "u:$5$")

	out, err = run(t, "", "-nb5", "-r", "1000", "u", "p")
	require.NoError(t, err)
	require.Contains(t, out, "u:$6$")

	// -BinC 4 u: the README drop-in bundle (-B -i -n -C 4), password on stdin.
	out, err = run(t, "p", "-BinC", "4", "u")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(strings.TrimSpace(out), "u:$2y$04$"),
		"want u:$2y$04$ line, got %q", out)
}

// Test_cli_ConflictingFlagsRejected covers the mutually-exclusive matrix. d8
// rejects exactly the combinations real htpasswd rejects with
// "only one of -c -n -v -D may be specified" / usage (exit 2); d8 collapses all
// to a generic error (exit 1 at the root — see finding F2).
func Test_cli_ConflictingFlagsRejected(t *testing.T) {
	pairs := [][]string{
		{"-c", "-n", "f", "u"},
		{"-n", "-D", "f", "u"},
		{"-n", "-v", "f", "u"},
		{"-v", "-c", "f", "u"},
		{"-v", "-D", "f", "u"},
		{"-c", "-D", "f", "u"},
		{"-b", "-i", "f", "u", "p"},
	}
	for _, args := range pairs {
		_, err := run(t, "", args...)
		require.Error(t, err, "expected rejection for %v", args)
	}
}

// Test_cli_MultipleAlgorithmsRejected: d8 is stricter than htpasswd here.
// Real htpasswd accepts several algorithm flags and applies the last one
// (exit 0); d8 rejects any second algorithm flag (finding F5).
func Test_cli_MultipleAlgorithmsRejected(t *testing.T) {
	_, err := run(t, "", "-nb", "-B", "-m", "u", "p")
	require.Error(t, err)
	_, err = run(t, "", "-nb", "-m", "-B", "u", "p") // order-independent in d8
	require.Error(t, err)
}

// Test_cli_CostRequiresBcrypt: -C with an explicit non-bcrypt algorithm is an
// error in d8. DIVERGENCE (F4): real htpasswd warns "Ignoring -C argument for
// this algorithm" and succeeds (exit 0). The second case documents the other
// side of the same coin: because bcrypt is d8's default, -C works WITHOUT -B
// (real htpasswd would ignore -C without -B).
func Test_cli_CostRequiresBcrypt(t *testing.T) {
	_, err := run(t, "", "-nb", "-m", "-C", "8", "u", "p")
	require.Error(t, err)

	out, err := run(t, "p", "-n", "-C", "4") // default alg is bcrypt
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(strings.TrimSpace(out), "$2y$04$"),
		"default-bcrypt -C should apply, got %q", out)
}

// Test_cli_RoundsRequiresShaCrypt: -r is valid only with -2/-5 (a d8 extension
// with no htpasswd equivalent at all).
func Test_cli_RoundsRequiresShaCrypt(t *testing.T) {
	_, err := run(t, "", "-nb", "-r", "5000", "u", "p") // default bcrypt
	require.Error(t, err)
	_, err = run(t, "", "-nb", "-B", "-r", "5000", "u", "p")
	require.Error(t, err)

	out, err := run(t, "", "-nb", "-2", "-r", "5000", "u", "p")
	require.NoError(t, err)
	require.Contains(t, out, "u:$5$")
}

// Test_cli_CostRangeBounds: d8 accepts bcrypt cost 4..31 (bcrypt.MinCost..MaxCost).
// NOTE (F7): real htpasswd only accepts 4..17 and rejects 18..31 (exit 3); we
// test d8's own documented bounds and keep the accepted case at cost 4 for speed.
func Test_cli_CostRangeBounds(t *testing.T) {
	_, err := run(t, "", "-nb", "-B", "-C", "3", "u", "p") // below min
	require.Error(t, err)
	_, err = run(t, "", "-nb", "-B", "-C", "32", "u", "p") // above max
	require.Error(t, err)

	out, err := run(t, "", "-nb", "-B", "-C", "4", "u", "p")
	require.NoError(t, err)
	require.Contains(t, out, "u:$2y$04$")
}

// Test_cli_RoundsRangeBounds: d8 bounds SHA-crypt rounds at 1000..999999999.
func Test_cli_RoundsRangeBounds(t *testing.T) {
	_, err := run(t, "", "-nb", "-5", "-r", "999", "u", "p")
	require.Error(t, err)
	_, err = run(t, "", "-nb", "-5", "-r", "1000000000", "u", "p")
	require.Error(t, err)

	out, err := run(t, "", "-nb", "-5", "-r", "1000", "u", "p")
	require.NoError(t, err)
	require.Contains(t, out, "u:$6$rounds=1000$")
}

// Test_cli_StdoutForms exercises the three -n username forms and the batch
// arg-count rules.
func Test_cli_StdoutForms(t *testing.T) {
	// Bare (1 batch arg = password): no colon, hash matches the password.
	out, err := run(t, "", "-nb", "-C", "4", "solopass")
	require.NoError(t, err)
	bare := strings.TrimSpace(out)
	require.False(t, strings.Contains(bare, ":"), "bare hash must have no colon, got %q", bare)
	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(bare), []byte("solopass")))

	// Username present (2 batch args): "user:hash".
	out, err = run(t, "", "-nb", "-C", "4", "alice", "p")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(strings.TrimSpace(out), "alice:$2y$04$"), "got %q", out)

	// Explicit empty username: ":hash", exactly like `htpasswd -n ""` (exit 0).
	out, err = run(t, "", "-nb", "-C", "4", "", "p")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(strings.TrimSpace(out), ":$2y$04$"), "got %q", out)

	// Too many args for non-batch -n, and zero args for -nb: both rejected.
	_, err = run(t, "", "-n", "-i", "a", "b")
	require.Error(t, err)
	_, err = run(t, "", "-nb", "-C", "4")
	require.Error(t, err)
}

// Test_cli_BatchEmptyPassword: PARITY. Real `htpasswd -nb user ""` succeeds
// (exit 0) and so must d8 — the batch path takes the password argument verbatim
// with no empty-string check (finding F3, batch side).
func Test_cli_BatchEmptyPassword(t *testing.T) {
	out, err := run(t, "", "-nb", "-C", "4", "user", "")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(strings.TrimSpace(out), "user:$2y$04$"), "got %q", out)
}

// Test_cli_StdinEmptyPasswordRejected documents finding F3 (stdin side): d8
// rejects an empty password read from stdin/-i, whereas real htpasswd ACCEPTS
// it (`printf ” | htpasswd -ni user` -> exit 0). run() swaps stdin only for a
// non-empty string, so "\n" yields a pipe whose first line is empty.
func Test_cli_StdinEmptyPasswordRejected(t *testing.T) {
	_, err := run(t, "\n", "-n", "-i", "-C", "4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

// Test_cli_UsernameRules: ':' and control chars rejected; the byte-length cap.
// NOTE (F9): the limit is bytes, so 128 two-byte runes (256 bytes) is rejected
// though it is only 128 characters; and real htpasswd rejects a 255-byte name
// ("resultant record too long", exit 5) that d8 accepts.
func Test_cli_UsernameRules(t *testing.T) {
	_, err := run(t, "", "-nb", "-C", "4", "us:er", "p")
	require.Error(t, err)
	require.Contains(t, err.Error(), ":")

	_, err = run(t, "", "-nb", "-C", "4", "a\tb", "p")
	require.Error(t, err)

	_, err = run(t, "", "-nb", "-C", "4", strings.Repeat("u", 256), "p")
	require.Error(t, err)

	_, err = run(t, "", "-nb", "-C", "4", strings.Repeat("é", 128), "p") // 256 bytes
	require.Error(t, err)

	// 255 bytes is accepted by d8 (htpasswd would reject as record-too-long).
	out, err := run(t, "", "-nb", "-C", "4", strings.Repeat("u", 255), "p")
	require.NoError(t, err)
	require.Contains(t, out, ":$2y$04$")
}

// Test_cli_MissingFileErrors: add/verify/delete against a nonexistent file all
// error (real htpasswd: "cannot modify file X; use '-c' to create it", exit 1).
func Test_cli_MissingFileErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.htpasswd")

	_, err := run(t, "", "-b", "-C", "4", missing, "u", "p")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
	require.Contains(t, err.Error(), "-c")

	_, err = run(t, "", "-bv", missing, "u", "p")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")

	_, err = run(t, "", "-D", missing, "u")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

// Test_cli_LifecycleMessages locks the add/update/verify/delete messages, which
// match real htpasswd verbatim ("Adding/Updating/Deleting password for user X",
// "Password for user X correct."). run() folds stderr into the returned string.
func Test_cli_LifecycleMessages(t *testing.T) {
	file := filepath.Join(t.TempDir(), "u.htpasswd")

	out, err := run(t, "", "-cb", "-C", "4", file, "alice", "p1")
	require.NoError(t, err)
	require.Contains(t, out, "Adding password for user alice")

	out, err = run(t, "", "-b", "-C", "4", file, "alice", "p2")
	require.NoError(t, err)
	require.Contains(t, out, "Updating password for user alice")

	out, err = run(t, "", "-bv", file, "alice", "p2")
	require.NoError(t, err)
	require.Contains(t, out, "Password for user alice correct.")

	_, err = run(t, "", "-bv", file, "alice", "WRONG")
	require.Error(t, err)
	require.Contains(t, err.Error(), "verification failed")

	_, err = run(t, "", "-bv", file, "ghost", "x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	out, err = run(t, "", "-D", file, "alice")
	require.NoError(t, err)
	require.Contains(t, out, "Deleting password for user alice")

	_, err = run(t, "", "-D", file, "ghost")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// Test_cli_EmptyUsernameRejectedForFileOps covers the fix for the empty-username
// collision: add/update, delete and verify against a file now reject an empty
// username (which would otherwise clobber blank lines in the file), while the
// '-n' stdout mode still accepts an explicit empty username and prints ":hash"
// exactly like `htpasswd -n ""`.
func Test_cli_EmptyUsernameRejectedForFileOps(t *testing.T) {
	file := filepath.Join(t.TempDir(), "u.htpasswd")

	// -c create (batch) with an empty username is rejected before the file is written.
	_, err := run(t, "", "-cb", "-C", "4", file, "", "pw")
	require.Error(t, err)
	require.Contains(t, err.Error(), "username must not be empty")

	// The plaintext form from the original bug report is rejected too.
	_, err = run(t, "", "-cb", "-p", file, "", "pw")
	require.Error(t, err)
	require.Contains(t, err.Error(), "username must not be empty")

	// Seed a real user, then confirm delete and verify also reject "".
	_, err = run(t, "", "-cb", "-C", "4", file, "alice", "pw")
	require.NoError(t, err)

	_, err = run(t, "", "-D", file, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "username must not be empty")

	_, err = run(t, "", "-bv", file, "", "pw")
	require.Error(t, err)
	require.Contains(t, err.Error(), "username must not be empty")

	// -n stdout mode still allows an explicit empty username (":hash").
	out, err := run(t, "", "-nb", "-C", "4", "", "pw")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(strings.TrimSpace(out), ":$2y$04$"), "got %q", out)
}

// cli_exitCode extracts the process exit code a command's error carries via the
// ExitCode() method (see internal/tools/htpasswd/exit.go). Errors without one —
// e.g. file-access failures — map to 1, exactly as cmd/d8/root.go decides.
func cli_exitCode(t *testing.T, err error) int {
	t.Helper()
	require.Error(t, err)

	var coder interface{ ExitCode() int }
	if errors.As(err, &coder) {
		return coder.ExitCode()
	}

	return 1
}

// Test_cli_ExitCodes locks the Apache-htpasswd-compatible exit codes d8 now
// returns: 2 usage/syntax, 3 verification failure, 5 over-long username, 6
// bad/absent user, and 1 for file-access errors.
func Test_cli_ExitCodes(t *testing.T) {
	file := filepath.Join(t.TempDir(), "u.htpasswd")
	_, err := run(t, "", "-cb", "-C", "4", file, "alice", "pw")
	require.NoError(t, err)

	cases := []struct {
		name  string
		want  int
		stdin string
		args  []string
	}{
		{"conflicting_flags", 2, "", []string{"-c", "-n", "f", "u"}},
		{"two_algorithms", 2, "", []string{"-nb", "-B", "-m", "u", "p"}},
		{"cost_without_bcrypt", 2, "", []string{"-nb", "-m", "-C", "8", "u", "p"}},
		{"cost_out_of_range", 2, "", []string{"-nb", "-B", "-C", "99", "u", "p"}},
		{"rounds_without_shacrypt", 2, "", []string{"-nb", "-r", "5000", "u", "p"}},
		{"colon_in_username", 6, "", []string{"-nb", "-C", "4", "a:b", "p"}},
		{"username_too_long", 5, "", []string{"-nb", "-C", "4", strings.Repeat("u", 256), "p"}},
		{"verify_wrong_password", 3, "", []string{"-bv", file, "alice", "WRONG"}},
		{"verify_user_not_found", 6, "", []string{"-bv", file, "ghost", "x"}},
		{"empty_username_file", 2, "", []string{"-cb", "-C", "4", file, "", "pw"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := run(t, c.stdin, c.args...)
			require.Equal(t, c.want, cli_exitCode(t, err), "exit code for %v", c.args)
		})
	}

	// A missing password file (no -c) is a file-access error: exit 1, unwrapped.
	_, err = run(t, "", "-b", "-C", "4", filepath.Join(t.TempDir(), "nope"), "u", "p")
	require.Equal(t, 1, cli_exitCode(t, err), "missing file must be exit 1")
}
