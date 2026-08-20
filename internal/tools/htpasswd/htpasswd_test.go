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
	"golang.org/x/crypto/bcrypt"
)

// Test_generateHash_RoundTrip checks that every algorithm produces a hash that
// verifyHash accepts for the right password and rejects for a wrong one.
func Test_generateHash_RoundTrip(t *testing.T) {
	const (
		password = "Test12345!"
		// wrong must differ within the first 8 bytes so it is also rejected by
		// DES crypt, which ignores everything past byte 8.
		wrong = "Xest12345?"
	)

	algs := []algorithm{algBcrypt, algAPR1, algSHA256, algSHA512, algDES, algSHA1, algPlain}
	for _, alg := range algs {
		t.Run(string(alg), func(t *testing.T) {
			hash, err := generateHash(password, hashOptions{alg: alg, cost: DefaultBcryptCost})
			require.NoError(t, err)
			require.NotEmpty(t, hash)

			ok, err := verifyHash(password, hash)
			require.NoError(t, err)
			require.True(t, ok, "correct password should verify against %q", hash)

			// DES only considers the first 8 bytes, so pick a wrong password
			// that differs within that window.
			ok, err = verifyHash(wrong, hash)
			require.NoError(t, err)
			require.False(t, ok, "wrong password should not verify against %q", hash)
		})
	}
}

func Test_bcryptHash_EmitsHtpasswdIdentifierAndCost(t *testing.T) {
	hash, err := bcryptHash("Test12345!", 12)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(hash, "$2y$"), "expected $2y$ prefix, got %q", hash)

	cost, err := bcrypt.Cost([]byte(hash))
	require.NoError(t, err)
	require.Equal(t, 12, cost)

	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(hash), []byte("Test12345!")))
}

// Test_verifyHash_AcceptsForeignHashes verifies against hashes produced by the
// system reference tools, proving cross-implementation compatibility.
func Test_verifyHash_AcceptsForeignHashes(t *testing.T) {
	cases := []struct{ name, stored string }{
		{"apr1", "$apr1$SsFduAdd$N8RB421wyIBb686LI12ko."},
		{"md5", "$1$saltsalt$qjXMvbEw8oaL.CzflDtaK/"},
		{"sha256", "$5$saltsalt$gOjOtoMpVhru2uyjeJSEc/JaLQWOXMNmlOnj6T4AtC."},
		{"sha512", "$6$saltsalt$qFmFH.bQmmtXzyBY0s9v7Oicd2z4XSIecDzlB5KiA2/jctKu9YterLp8wwnSq.qc.eoxqOmSuNp2xS0ktL3nh/"},
		{"des", "abJnggxhB/yWI"},
		{"sha256-rounds", "$5$rounds=10000$saltsalt$a6WJS3V6B3leg7T3.ELC5.vcUmHOyFDvLaurLBy.mc8"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, err := verifyHash("password", c.stored)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = verifyHash("wrongpass", c.stored)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

func Test_validateUsername(t *testing.T) {
	require.NoError(t, validateUsername("alice"))
	require.NoError(t, validateUsername("")) // allowed: bare-hash stdout mode
	require.Error(t, validateUsername("ali:ce"))
	require.Error(t, validateUsername("ali\nce"))
	require.Error(t, validateUsername(strings.Repeat("x", 256)))
}

func Test_readPasswordLine(t *testing.T) {
	pw, err := readPasswordLine(strings.NewReader("Test12345!\n"))
	require.NoError(t, err)
	require.Equal(t, "Test12345!", pw)

	_, err = readPasswordLine(strings.NewReader("\n"))
	require.Error(t, err)

	_, err = readPasswordLine(strings.NewReader(""))
	require.Error(t, err)
}
