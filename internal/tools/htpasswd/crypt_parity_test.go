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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// All expected values below were produced independently by the system reference
// implementations and cross-checked against the Go port:
//   - DES:  libxcrypt 4.4.36 crypt(3) (via a C harness) + perl crypt
//   - $1$:  python3 crypt (glibc/libxcrypt) + `openssl passwd -1`
//   - apr1: `openssl passwd -apr1` + Apache `htpasswd -vb` round-trip
//   - $5$/$6$: python3 crypt (SHA256/SHA512) + `openssl passwd -5/-6`
// A mismatch means the port diverged from the canonical algorithm.

func Test_crypt_DESEdgeCases(t *testing.T) {
	cases := []struct{ name, password, salt, want string }{
		{"empty_pw", "", "ab", "abmF1QH4PEr.E"},
		{"len_lt_8", "abc", "ab", "abFZSxKKdq5s6"},
		{"len_eq_8", "abcdefgh", "ab", "abYH7TYgEKz2Q"},
		{"len_gt_8_first8_only", "abcdefghIJKL", "ab", "abYH7TYgEKz2Q"},
		{"salt_dotdot", "password", "..", "..UZoIyj/Hy/c"},
		{"salt_slashslash", "password", "//", "//TIk/siaNpyQ"},
		{"salt_zz", "password", "zz", "zzXUHfURnGg8I"},
		{"password_ab", "password", "ab", "abJnggxhB/yWI"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, desCrypt(c.password, c.salt))
		})
	}

	// Only the first 8 password bytes matter: >8 must equal the 8-byte prefix.
	require.Equal(t,
		desCrypt("abcdefgh", "ab"),
		desCrypt("abcdefghIJKL", "ab"),
		"DES must ignore password bytes beyond the 8th")

	// A 1-char salt is padded with '.' (internal-consistency; libxcrypt rejects
	// a 1-char salt outright, so this pins the port's lenient behavior).
	require.Equal(t,
		desCrypt("password", "a."),
		desCrypt("password", "a"),
		"1-char DES salt should be padded to \"a.\"")
}

func Test_crypt_MD5EdgeCases(t *testing.T) {
	cases := []struct{ name, password, salt, magic, want string }{
		{"apr1_basic", "password", "SsFduAdd", magicAPR1, "$apr1$SsFduAdd$N8RB421wyIBb686LI12ko."},
		{"apr1_test123", "Test123!", "abcdefgh", magicAPR1, "$apr1$abcdefgh$gj2HqWsjGbOdAts0DpThK."},
		{"apr1_empty_pw", "", "abcdefgh", magicAPR1, "$apr1$abcdefgh$L.PT565ESX4Tp2bqNs7Ie."},
		{"apr1_short_salt", "password", "abc", magicAPR1, "$apr1$abc$mehJE/UcwZsj.w5DYe.b5."},
		// salt longer than 8 is truncated to 8 both here and in the reference.
		{"apr1_salt_trunc8", "password", "abcdefghij", magicAPR1, "$apr1$abcdefgh$FBwExRW4dCc8aL.OvjpIE1"},
		// multibyte/high-bit password ("päss" UTF-8: 70 c3 a4 73 73).
		{"apr1_utf8", "päss", "saltsalt", magicAPR1, "$apr1$saltsalt$f74.33Z0f34Aak2NYQJTa0"},
		{"md5_basic", "password", "saltsalt", magicMD5, "$1$saltsalt$qjXMvbEw8oaL.CzflDtaK/"},
		{"md5_empty_pw", "", "saltsalt", magicMD5, "$1$saltsalt$5Jhcit4zN9UlGiA0txPkO0"},
		{"md5_utf8", "päss", "saltsalt", magicMD5, "$1$saltsalt$npHVA9aR/rej/t9wc6U4V0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, md5Crypt(c.password, c.salt, c.magic))
		})
	}

	// salt > 8 must hash identically to the 8-byte prefix.
	require.Equal(t,
		md5Crypt("password", "abcdefgh", magicAPR1),
		md5Crypt("password", "abcdefghij", magicAPR1),
		"MD5 salt must be truncated to 8 bytes")
}

func Test_crypt_SHA256Vectors(t *testing.T) {
	cases := []struct {
		name, password, salt string
		rounds               int
		want                 string
	}{
		{"default", "password", "saltsalt", 0, "$5$saltsalt$gOjOtoMpVhru2uyjeJSEc/JaLQWOXMNmlOnj6T4AtC."},
		{"rounds_10000", "password", "saltsalt", 10000, "$5$rounds=10000$saltsalt$a6WJS3V6B3leg7T3.ELC5.vcUmHOyFDvLaurLBy.mc8"},
		{"rounds_1000", "password", "saltsalt", 1000, "$5$rounds=1000$saltsalt$azOwbpkvuuBKkE82dQPwTsQE8JyT9Fflpr9aKid3aT9"},
		{"long_pw_40b", "0123456789012345678901234567890123456789", "saltsalt", 0, "$5$saltsalt$wFv09ZBxx3UoYQk5Z1RCJ5ZTGv13Sp1r1vIjpms6Bv7"},
		// salt > 16 truncated to 16 (both here and in the reference).
		{"salt_trunc16", "password", "abcdefghijklmnopqrstuvwxyz", 0, "$5$abcdefghijklmnop$ieyonWfl7MR75BuN79Fkt2PqhPI43TsNZYGUObDGVI/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, shaCrypt(c.password, c.salt, c.rounds, false))
		})
	}
}

func Test_crypt_SHA512Vectors(t *testing.T) {
	cases := []struct {
		name, password, salt string
		rounds               int
		want                 string
	}{
		{"default", "password", "saltsalt", 0, "$6$saltsalt$qFmFH.bQmmtXzyBY0s9v7Oicd2z4XSIecDzlB5KiA2/jctKu9YterLp8wwnSq.qc.eoxqOmSuNp2xS0ktL3nh/"},
		{"rounds_10000", "password", "saltsalt", 10000, "$6$rounds=10000$saltsalt$ZqOTO2O04D/DgwZlm.rZTgWxvBaIf4LQsZKtXFEu9UHJ4CvgmdLAGxKUzJ0mPO98OevETdY6oK/Oac6j2Axxq/"},
		{"salt16_test123", "Test123!", "abcdefghijklmnop", 0, "$6$abcdefghijklmnop$Vthr3YXPXseV5egL67KCgMNLr7uYIxy6j/lec/PGvO5oJWeGG/ZXLCHkfFp9nryV.VdKV/0fzFJwmOSHHocNf1"},
		{"long_pw_70b", "0123456789012345678901234567890123456789012345678901234567890123456789", "saltsalt", 0, "$6$saltsalt$jj0/1v8ZaK/nhCM8sBULJrCvrRsaVIUaM1mgnnIfKp4etInK1E7h1ZmhgswwKEFEYCI2mnQlNIglg0yUILTDv."},
		{"salt_trunc16", "password", "abcdefghijklmnopqrstuvwxyz", 0, "$6$abcdefghijklmnop$0aenUFHf897F9u0tURIHOeACWajSuVGa7jgJGyq.DKZm/WXl/IZFvPbneFydBjomEOgM.Sh1m0L3KsS1.H5b//"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, shaCrypt(c.password, c.salt, c.rounds, true))
		})
	}
}

// SHA-crypt omits the rounds= field at the default (rounds arg == 0) but emits
// it whenever rounds are given explicitly - even when the explicit value equals
// the 5000 default. The checksum is identical either way (same work factor);
// only the prefix differs. This matches glibc/libxcrypt's rounds_custom flag.
func Test_crypt_SHARoundsFormatting(t *testing.T) {
	def5 := shaCrypt("password", "saltsalt", 0, false)
	exp5 := shaCrypt("password", "saltsalt", 5000, false)
	require.Equal(t, "$5$saltsalt$gOjOtoMpVhru2uyjeJSEc/JaLQWOXMNmlOnj6T4AtC.", def5)
	require.Equal(t, "$5$rounds=5000$saltsalt$gOjOtoMpVhru2uyjeJSEc/JaLQWOXMNmlOnj6T4AtC.", exp5)
	require.NotContains(t, def5, "rounds=", "default must omit the rounds= field")
	require.Contains(t, exp5, "rounds=5000$", "explicit rounds must be emitted even when == default")

	// Same for $6$.
	def6 := shaCrypt("password", "saltsalt", 0, true)
	exp6 := shaCrypt("password", "saltsalt", 5000, true)
	require.NotContains(t, def6, "rounds=")
	require.Contains(t, exp6, "rounds=5000$")
	// Identical checksum tail regardless of the rounds= prefix.
	require.Equal(t,
		strings.TrimPrefix(def6, "$6$saltsalt$"),
		strings.TrimPrefix(exp6, "$6$rounds=5000$saltsalt$"))
}

// The port CLAMPS rounds to [1000, 999999999] per Drepper's SHA-crypt spec and
// classic glibc. (Note: libxcrypt 4.4.x instead REJECTS out-of-range rounds,
// returning "*0" - a documented divergence for out-of-range inputs only.)
func Test_crypt_SHARoundsClamp(t *testing.T) {
	// Below the minimum clamps up to 1000.
	require.Equal(t,
		shaCrypt("password", "saltsalt", 1000, false),
		shaCrypt("password", "saltsalt", 500, false))
	require.Equal(t,
		"$5$rounds=1000$saltsalt$azOwbpkvuuBKkE82dQPwTsQE8JyT9Fflpr9aKid3aT9",
		shaCrypt("password", "saltsalt", 500, false))
	// NOTE: the upper clamp (rounds > 999999999 -> 999999999) is verified by
	// inspection of shacrypt.go only; executing it would run ~1e9 SHA rounds.
}

func Test_crypt_To64Encoding(t *testing.T) {
	// to64 appends n crypt64 chars encoding the low 6*n bits, least-significant
	// group first. crypt64 = "./0-9A-Za-z", so index 0='.', 1='/', 63='z'.
	require.Equal(t, ".", string(to64(nil, 0, 1)))
	require.Equal(t, "z", string(to64(nil, 63, 1)))
	require.Equal(t, ".", string(to64(nil, 64, 1))) // 64 & 0x3f == 0
	require.Equal(t, "/.", string(to64(nil, 1, 2))) // LSB group first: 1 -> '/', then 0 -> '.'
	require.Equal(t, "zzzz", string(to64(nil, 0xffffff, 4)))
}

func Test_crypt_Crypt64Alphabet(t *testing.T) {
	require.Equal(t, 64, len(crypt64))
	require.Equal(t, "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", crypt64)
	require.Equal(t, 0, crypt64Index('.'))
	require.Equal(t, 1, crypt64Index('/'))
	require.Equal(t, 2, crypt64Index('0'))
	require.Equal(t, 12, crypt64Index('A'))
	require.Equal(t, 63, crypt64Index('z'))
	// Non-alphabet bytes silently map to 0 (same slot as '.').
	require.Equal(t, 0, crypt64Index('!'))
	require.Equal(t, 0, crypt64Index('='))

	require.True(t, isCrypt64("abJnggxhB/yWI"))
	require.True(t, isCrypt64(""))
	require.False(t, isCrypt64("abc!"))
	require.False(t, isCrypt64("ab=="))
}

// randomSalt is unbiased: len(crypt64)==64 divides 256 exactly, so byte%64 maps
// exactly 4 source bytes to each of the 64 symbols - no modulo bias.
func Test_crypt_RandomSaltUnbiased(t *testing.T) {
	const draws = 4000
	seen := map[rune]int{}
	for range draws {
		s, err := randomSalt(16)
		require.NoError(t, err)
		require.Len(t, s, 16)
		require.True(t, isCrypt64(s), "salt %q must be all-crypt64", s)
		for _, r := range s {
			seen[r]++
		}
	}
	// Every one of the 64 symbols must appear across 64k samples.
	require.Len(t, seen, 64, "all 64 crypt64 symbols should be reachable")
}

func Test_crypt_ParseMD5RoundTrip(t *testing.T) {
	for _, tc := range []struct{ pw, salt, magic string }{
		{"password", "SsFduAdd", magicAPR1},
		{"password", "saltsalt", magicMD5},
		{"", "abcdefgh", magicAPR1},
	} {
		h := md5Crypt(tc.pw, tc.salt, tc.magic)
		magic, salt, ok := parseMD5Crypt(h)
		require.True(t, ok)
		require.Equal(t, tc.magic, magic)
		require.Equal(t, h, md5Crypt(tc.pw, salt, magic), "re-hash must reproduce the hash")
	}

	// Malformed / unsupported inputs are rejected.
	_, _, ok := parseMD5Crypt("$2y$notmd5")
	require.False(t, ok)
	_, _, ok = parseMD5Crypt("$apr1$noDollarSalt")
	require.False(t, ok, "missing checksum separator must be rejected")
}

func Test_crypt_ParseShaRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hash  string
		wantR int
		wantS string
		want5 bool
	}{
		{"s5_no_rounds", shaCrypt("password", "saltsalt", 0, false), 0, "saltsalt", false},
		{"s5_rounds", shaCrypt("password", "saltsalt", 10000, false), 10000, "saltsalt", false},
		{"s6_no_rounds", shaCrypt("password", "saltsalt", 0, true), 0, "saltsalt", true},
		{"s6_rounds", shaCrypt("password", "abcdefghijklmnop", 10000, true), 10000, "abcdefghijklmnop", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rounds, salt, is512, ok := parseShaCrypt(tc.hash)
			require.True(t, ok)
			require.Equal(t, tc.wantR, rounds)
			require.Equal(t, tc.wantS, salt)
			require.Equal(t, tc.want5, is512)
			// Re-hash with the parsed params must reproduce the original.
			require.Equal(t, tc.hash, shaCrypt("password", salt, rounds, is512))
		})
	}

	// Malformed inputs.
	_, _, _, ok := parseShaCrypt("$7$saltsalt$xxxx")
	require.False(t, ok, "unknown scheme must be rejected")
	_, _, _, ok = parseShaCrypt("$5$rounds=notanumber$saltsalt$xxxx")
	require.False(t, ok, "non-numeric rounds must be rejected")
}
