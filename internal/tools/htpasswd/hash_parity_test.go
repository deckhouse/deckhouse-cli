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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// Test_hash_BcryptRewriteAcrossCosts confirms the $2a$ -> $2y$ rewrite in
// bcryptHash (hash.go:102, `"$2y$" + string(h[4:])`) is correct at every cost.
// Go's bcrypt always emits a fixed 4-byte "$2a$" prefix (major '2' + minor 'a'
// are constants, x/crypto bcrypt.go:57-58,144-145) followed by a zero-padded
// two-digit cost (bcrypt.go:254, fmt.Sprintf("%02d", cost)), so slicing h[4:]
// drops exactly "$2a$" for all costs 4..31 and the rewrite is length-preserving
// (60 bytes total).
//
// NOTE: cost 31 is deliberately NOT exercised at runtime: bcrypt at cost 31 runs
// 2^31 key-setup rounds and takes many minutes-to-hours, which would hang CI.
// Its correctness follows structurally from the fixed prefix + %02d cost cited
// above (verified against x/crypto v0.54.0 source), so costs 4 and 10 are
// representative. If you must cover a high two-digit cost cheaply, use 18.
func Test_hash_BcryptRewriteAcrossCosts(t *testing.T) {
	const password = "Test12345!"
	for _, cost := range []int{bcrypt.MinCost, DefaultBcryptCost} { // 4, 10
		t.Run(fmt.Sprintf("cost%d", cost), func(t *testing.T) {
			h, err := bcryptHash(password, cost)
			require.NoError(t, err)

			// Structure parity with Apache `htpasswd -nbB`: $2y$ + 2-digit cost +
			// $ + 22-char salt + 31-char hash = 60 bytes.
			require.True(t, strings.HasPrefix(h, "$2y$"), "prefix, got %q", h)
			require.Len(t, h, 60, "bcrypt hash must be 60 bytes")
			require.Equal(t, fmt.Sprintf("%02d", cost), h[4:6], "zero-padded 2-digit cost")
			require.Equal(t, byte('$'), h[6], "cost delimiter")

			// Encoded cost round-trips and the rewritten $2y$ still verifies.
			got, err := bcrypt.Cost([]byte(h))
			require.NoError(t, err)
			require.Equal(t, cost, got)
			require.NoError(t, bcrypt.CompareHashAndPassword([]byte(h), []byte(password)),
				"rewritten $2y$ hash must verify with the underlying bcrypt lib")
		})
	}
}

// Test_hash_BcryptPasswordLength pins the >72-byte divergence from Apache.
//
// Go's bcrypt.GenerateFromPassword returns bcrypt.ErrPasswordTooLong for any
// password > 72 bytes (x/crypto bcrypt.go:96-98), so bcryptHash surfaces an
// error and produces no hash. Apache `htpasswd -nbB` instead SILENTLY TRUNCATES
// the password to 72 bytes and emits a hash (verified with the system binary:
// a 73-byte password verifies against a hash generated from its 72-byte prefix).
//
// This asserts the ACTUAL d8 behavior (error at 73+), which is the safer of the
// two: silent truncation collides distinct long passwords onto one credential.
// The divergence is a documented parity gap, not a bug.
func Test_hash_BcryptPasswordLength(t *testing.T) {
	// <= 72 bytes: accepted.
	for _, n := range []int{71, 72} {
		h, err := bcryptHash(strings.Repeat("a", n), bcrypt.MinCost)
		require.NoErrorf(t, err, "len=%d must hash", n)
		require.NotEmpty(t, h)
	}
	// > 72 bytes: rejected with ErrPasswordTooLong (Apache would truncate).
	for _, n := range []int{73, 100} {
		h, err := bcryptHash(strings.Repeat("a", n), bcrypt.MinCost)
		require.Errorf(t, err, "len=%d must be rejected", n)
		require.ErrorIs(t, err, bcrypt.ErrPasswordTooLong)
		require.Empty(t, h)
	}
}

// Test_hash_Sha1ExactVector pins the unsalted {SHA} scheme to exact vectors that
// match both `htpasswd -nbs` and
// `printf %s pw | openssl dgst -sha1 -binary | openssl base64`.
func Test_hash_Sha1ExactVector(t *testing.T) {
	// htpasswd -nbs user password  ->  {SHA}W6ph5Mm5Pz8GgiULbPgzG37mj9g=
	require.Equal(t, "{SHA}W6ph5Mm5Pz8GgiULbPgzG37mj9g=", sha1Hash("password"))
	// SHA-1 of the empty string.
	require.Equal(t, "{SHA}2jmj7l5rSw0yVb/vlWAYkK/YBwk=", sha1Hash(""))

	// And it verifies through the auto-detecting verifier.
	ok, err := verifyHash("password", "{SHA}W6ph5Mm5Pz8GgiULbPgzG37mj9g=")
	require.NoError(t, err)
	require.True(t, ok)
}

// Test_hash_VerifyAutoDetectPerScheme extends the existing foreign-hash set with
// the two schemes it omits (bcrypt straight from Apache htpasswd, and {SHA}) and
// re-confirms each prefix routes to the right verifier. Every vector below is
// for the password "password"; the exact producing command is in the comment.
func Test_hash_VerifyAutoDetectPerScheme(t *testing.T) {
	cases := []struct{ name, stored string }{
		// htpasswd -nbB -C 5 user password  (salt is random; any one capture works)
		{"bcrypt_2y_from_htpasswd", "$2y$05$3wSYOLuQydmAcw/bACP03uc04PZQ2zAVU6qvmAnhJLN7Jul0kin6e"},
		// A $2a$ variant must route to bcrypt too (Go's own identifier).
		{"bcrypt_2a", hash_mustBcrypt2a(t, "password")},
		// htpasswd -nbs user password  ==  printf %s password|openssl dgst -sha1 -binary|openssl base64
		{"sha1_SHA", "{SHA}W6ph5Mm5Pz8GgiULbPgzG37mj9g="},
		// openssl passwd -apr1 -salt SsFduAdd password
		{"apr1", "$apr1$SsFduAdd$N8RB421wyIBb686LI12ko."},
		// openssl passwd -1 -salt saltsalt password
		{"md5_1", "$1$saltsalt$qjXMvbEw8oaL.CzflDtaK/"},
		// python3 -c "import crypt;print(crypt.crypt('password','$5$saltsalt'))"
		{"sha256_5", "$5$saltsalt$gOjOtoMpVhru2uyjeJSEc/JaLQWOXMNmlOnj6T4AtC."},
		// python3 crypt with an explicit rounds= field
		{"sha256_5_rounds", "$5$rounds=10000$saltsalt$a6WJS3V6B3leg7T3.ELC5.vcUmHOyFDvLaurLBy.mc8"},
		// python3 -c "import crypt;print(crypt.crypt('password','$6$saltsalt'))"
		{"sha512_6", "$6$saltsalt$qFmFH.bQmmtXzyBY0s9v7Oicd2z4XSIecDzlB5KiA2/jctKu9YterLp8wwnSq.qc.eoxqOmSuNp2xS0ktL3nh/"},
		// python3 -c "import crypt;print(crypt.crypt('password','ab'))"  (13-char DES)
		{"des", "abJnggxhB/yWI"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, err := verifyHash("password", c.stored)
			require.NoError(t, err)
			require.True(t, ok, "correct password must verify against %q", c.stored)

			// "wrongpass" differs in the first 8 bytes so DES (which ignores
			// bytes past 8) also rejects it.
			ok, err = verifyHash("wrongpass", c.stored)
			require.NoError(t, err)
			require.False(t, ok, "wrong password must be rejected by %q", c.stored)
		})
	}
}

// hash_mustBcrypt2a returns a raw Go bcrypt hash keeping its native "$2a$"
// identifier, to prove verifyHash routes $2a$ (not only the rewritten $2y$) to
// bcrypt.
func hash_mustBcrypt2a(t *testing.T, password string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(h), "$2a$"))
	return string(h)
}

// Test_hash_Verify13CharPlaintextAmbiguity proves the DES-then-plaintext
// fallthrough in verifyHash (hash.go:148-152) is correct.
//
//   - A 13-char crypt64 value that is NOT DES(password) falls through the DES
//     branch and is compared as plaintext -> a genuine 13-char '-p' password
//     verifies. (Real `htpasswd -v` CANNOT do this: it treats any bare 13-char
//     crypt64 string as DES and so fails to verify a 13-char plaintext entry.
//     d8 only ever accepts MORE than htpasswd here, never rejects a valid one.)
//   - A 13-char value that IS DES(password, salt) is accepted via the DES branch,
//     matching `htpasswd -v` exactly.
func Test_hash_Verify13CharPlaintextAmbiguity(t *testing.T) {
	// Sanity: the plaintext value is 13 crypt64 chars yet is not its own DES hash,
	// so the DES branch must miss and control must reach the plaintext compare.
	const plain = "abcdefghijklm"
	require.Len(t, plain, 13)
	require.True(t, isCrypt64(plain))
	require.NotEqual(t, plain, desCrypt(plain, plain[:2]),
		"precondition: plaintext must not collide with its own DES hash")

	// It is produced by generateHash(algPlain) verbatim, and verifies.
	stored, err := generateHash(plain, hashOptions{alg: algPlain})
	require.NoError(t, err)
	require.Equal(t, plain, stored)

	ok, err := verifyHash(plain, stored)
	require.NoError(t, err)
	require.True(t, ok, "13-char plaintext password must verify via fallthrough")

	// A different 13-char crypt64 password must be rejected.
	ok, err = verifyHash("nopqrstuvwxyz", stored)
	require.NoError(t, err)
	require.False(t, ok)

	// DES-interpretation branch: "abJnggxhB/yWI" == DES("password","ab"), so
	// verifyHash("password", ...) is accepted via the DES branch (parity with
	// `htpasswd -v`, which treats this bare 13-char value as DES too).
	ok, err = verifyHash("password", "abJnggxhB/yWI")
	require.NoError(t, err)
	require.True(t, ok)
}

// Test_hash_VerifyMalformed confirms malformed stored values never panic and
// never false-accept. The universal invariant is (ok == false) with no panic.
//
// A subtlety worth pinning: the error contract is NOT uniform. parseMD5Crypt
// (md5crypt.go:132-135) requires the "salt$hash" separator and so makes
// verifyHash return (false, err) for "$apr1$"; but parseShaCrypt
// (shacrypt.go:241-250) accepts a bare "$5$" as an empty-salt/no-checksum hash
// and returns ok=true, so verifyHash returns (false, NIL) for "$5$". Both are
// safe (no panic, no false-accept); they merely differ in whether an error is
// reported. wantErr below documents the actual, observed behavior of each input.
func Test_hash_VerifyMalformed(t *testing.T) {
	cases := []struct {
		stored  string
		wantErr bool
	}{
		{"$apr1$", true},                            // md5: no salt/hash after magic
		{"$apr1$onlysalt", true},                    // md5: missing '$hash' segment
		{"$1$", true},                               // md5: empty
		{"$5$", false},                              // sha-crypt: lenient -> (false,nil)
		{"$6$nodollarafterthis", false},             // sha-crypt: whole tail taken as salt
		{"$5$rounds=$saltsalt$deadbeef", true},      // sha-crypt: empty rounds value
		{"$5$rounds=notanumber$salt$hash", true},    // sha-crypt: non-numeric rounds
		{"$2y$", true},                              // bcrypt: truncated
		{"$2y$10$tooshort", true},                   // bcrypt: below minimum hash size
		{"$2a$99$" + strings.Repeat("x", 53), true}, // bcrypt: cost out of range
	}
	for _, c := range cases {
		t.Run(c.stored, func(t *testing.T) {
			var (
				ok  bool
				err error
			)
			require.NotPanics(t, func() { ok, err = verifyHash("password", c.stored) })
			require.False(t, ok, "malformed %q must never verify", c.stored)
			if c.wantErr {
				require.Error(t, err, "malformed %q must return an error", c.stored)
			}
		})
	}
}

// Test_hash_VerifyBcryptMismatchVsError distinguishes the two bcrypt paths in
// verifyHash (hash.go:117-127): a plain password mismatch is (false, nil), while
// a structurally invalid hash is (false, non-nil err).
func Test_hash_VerifyBcryptMismatchVsError(t *testing.T) {
	good, err := bcryptHash("password", bcrypt.MinCost)
	require.NoError(t, err)

	// Mismatch: valid hash, wrong password -> (false, nil).
	ok, err := verifyHash("WRONGpassword", good)
	require.NoError(t, err)
	require.False(t, ok)

	// Correct password -> (true, nil).
	ok, err = verifyHash("password", good)
	require.NoError(t, err)
	require.True(t, ok)

	// Structurally invalid bcrypt -> (false, err), not a bare mismatch.
	ok, err = verifyHash("password", "$2y$10$not-a-valid-bcrypt-hash")
	require.Error(t, err)
	require.False(t, ok)
}

// Test_hash_ConstEqAndIsCrypt64 pins the two helpers.
func Test_hash_ConstEqAndIsCrypt64(t *testing.T) {
	require.True(t, constEq("abc", "abc"))
	require.False(t, constEq("abc", "abd"))
	require.False(t, constEq("abc", "abcd"), "length mismatch must be false")
	require.False(t, constEq("", "x"))
	require.True(t, constEq("", ""))

	require.True(t, isCrypt64("abJnggxhB/yWI"))
	require.True(t, isCrypt64("./09AZaz"))
	require.False(t, isCrypt64("="), "'=' (base64 padding) is not in crypt64")
	require.False(t, isCrypt64("$1$"))
	require.False(t, isCrypt64("has space"))
	require.False(t, isCrypt64("colon:here"))
}

// Test_hash_GenerateDispatch confirms each algorithm emits the expected scheme
// marker, plaintext is verbatim, and an unknown algorithm errors.
func Test_hash_GenerateDispatch(t *testing.T) {
	const password = "Test12345!"
	checks := []struct {
		alg    algorithm
		verify func(t *testing.T, h string)
	}{
		{algBcrypt, func(t *testing.T, h string) { require.True(t, strings.HasPrefix(h, "$2y$")) }},
		{algAPR1, func(t *testing.T, h string) { require.True(t, strings.HasPrefix(h, "$apr1$")) }},
		{algSHA256, func(t *testing.T, h string) { require.True(t, strings.HasPrefix(h, "$5$")) }},
		{algSHA512, func(t *testing.T, h string) { require.True(t, strings.HasPrefix(h, "$6$")) }},
		{algSHA1, func(t *testing.T, h string) { require.True(t, strings.HasPrefix(h, "{SHA}")) }},
		{algDES, func(t *testing.T, h string) {
			require.Len(t, h, 13)
			require.True(t, isCrypt64(h))
		}},
		{algPlain, func(t *testing.T, h string) { require.Equal(t, password, h) }},
	}
	for _, c := range checks {
		t.Run(string(c.alg), func(t *testing.T) {
			h, err := generateHash(password, hashOptions{alg: c.alg, cost: DefaultBcryptCost})
			require.NoError(t, err)
			c.verify(t, h)
		})
	}

	_, err := generateHash(password, hashOptions{alg: algorithm("bogus")})
	require.Error(t, err, "unknown algorithm must error")
}

// Test_hash_VerifyLegacyBcryptIdentifiers confirms verifyHash routes the legacy
// bcrypt identifiers $2$ and $2x$ (alongside $2a$/$2b$/$2y$) to the bcrypt
// verifier. Go's bcrypt accepts any minor-version byte and computes the same
// hash regardless of the identifier, so these verify (empirically confirmed with
// x/crypto v0.54.0); before the fix they fell through to the plaintext branch
// and failed. Each variant is built from a fresh $2a$ hash by rewriting only the
// identifier, which does not change what bcrypt verifies for an ASCII password.
func Test_hash_VerifyLegacyBcryptIdentifiers(t *testing.T) {
	base, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.MinCost)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(base), "$2a$"))

	rest := string(base)[len("$2a$"):] // "04$<salt><hash>"
	for _, id := range []string{"$2$", "$2b$", "$2x$", "$2y$"} {
		stored := id + rest
		t.Run(id, func(t *testing.T) {
			ok, err := verifyHash("password", stored)
			require.NoError(t, err)
			require.True(t, ok, "correct password must verify against %q", stored)

			ok, err = verifyHash("wrongpassword", stored)
			require.NoError(t, err)
			require.False(t, ok, "wrong password must be rejected by %q", stored)
		})
	}
}
