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
	"testing"

	"github.com/stretchr/testify/require"
)

// These vectors were produced by the system's reference implementations
// (python3 crypt / glibc and `openssl passwd`) so a mismatch means our port
// diverged from the canonical algorithm, not merely from itself.

func Test_desCrypt_Vectors(t *testing.T) {
	cases := []struct{ password, salt, want string }{
		{"password", "ab", "abJnggxhB/yWI"},
		{"Test123!", "xy", "xyZz5eiXOP3r."},
		{"hello", "zz", "zzM3H1GzLNjgA"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, desCrypt(c.password, c.salt), "desCrypt(%q,%q)", c.password, c.salt)
	}
}

func Test_md5Crypt_Vectors(t *testing.T) {
	require.Equal(t,
		"$apr1$SsFduAdd$N8RB421wyIBb686LI12ko.",
		md5Crypt("password", "SsFduAdd", magicAPR1))
	require.Equal(t,
		"$apr1$abcdefgh$gj2HqWsjGbOdAts0DpThK.",
		md5Crypt("Test123!", "abcdefgh", magicAPR1))
	require.Equal(t,
		"$1$saltsalt$qjXMvbEw8oaL.CzflDtaK/",
		md5Crypt("password", "saltsalt", magicMD5))
}

func Test_shaCrypt_Vectors(t *testing.T) {
	// SHA-256 ($5$)
	require.Equal(t,
		"$5$saltsalt$gOjOtoMpVhru2uyjeJSEc/JaLQWOXMNmlOnj6T4AtC.",
		shaCrypt("password", "saltsalt", 0, false))
	require.Equal(t,
		"$5$rounds=10000$saltsalt$a6WJS3V6B3leg7T3.ELC5.vcUmHOyFDvLaurLBy.mc8",
		shaCrypt("password", "saltsalt", 10000, false))

	// SHA-512 ($6$)
	require.Equal(t,
		"$6$saltsalt$qFmFH.bQmmtXzyBY0s9v7Oicd2z4XSIecDzlB5KiA2/jctKu9YterLp8wwnSq.qc.eoxqOmSuNp2xS0ktL3nh/",
		shaCrypt("password", "saltsalt", 0, true))
	require.Equal(t,
		"$6$rounds=10000$saltsalt$ZqOTO2O04D/DgwZlm.rZTgWxvBaIf4LQsZKtXFEu9UHJ4CvgmdLAGxKUzJ0mPO98OevETdY6oK/Oac6j2Axxq/",
		shaCrypt("password", "saltsalt", 10000, true))
	require.Equal(t,
		"$6$abcdefghijklmnop$Vthr3YXPXseV5egL67KCgMNLr7uYIxy6j/lec/PGvO5oJWeGG/ZXLCHkfFp9nryV.VdKV/0fzFJwmOSHHocNf1",
		shaCrypt("Test123!", "abcdefghijklmnop", 0, true))
}
