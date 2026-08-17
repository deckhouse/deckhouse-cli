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
	"crypto/md5"
	"strings"
)

// magicAPR1 is Apache's MD5 variant marker (htpasswd -m); magicMD5 is the
// original FreeBSD/PHK md5crypt marker. The two produce different hashes for
// the same password because the magic is folded into the digest.
const (
	magicAPR1 = "$apr1$"
	magicMD5  = "$1$"
)

// md5Crypt implements the Poul-Henning Kamp MD5-based crypt algorithm used by
// both '$1$' (magicMD5) and Apache's '$apr1$' (magicAPR1). The salt is capped
// at 8 characters, matching the reference implementation. Returns the full
// "<magic><salt>$<checksum>" string.
func md5Crypt(password, salt, magic string) string {
	if len(salt) > 8 {
		salt = salt[:8]
	}

	pw := []byte(password)

	// Primary digest: password, magic, salt.
	primary := md5.New()
	primary.Write(pw)
	primary.Write([]byte(magic))
	primary.Write([]byte(salt))

	// Alternate digest: password, salt, password. Its bytes are folded back
	// into the primary digest, once per password byte.
	alt := md5.New()
	alt.Write(pw)
	alt.Write([]byte(salt))
	alt.Write(pw)
	altSum := alt.Sum(nil)

	for i := len(pw); i > 0; i -= 16 {
		if i > 16 {
			primary.Write(altSum[:16])
		} else {
			primary.Write(altSum[:i])
		}
	}

	// Fold the password length into the digest one bit at a time: a NUL byte
	// for a 1 bit, the first password byte for a 0 bit. This is the historical
	// quirk that makes md5crypt md5crypt.
	for i := len(pw); i > 0; i >>= 1 {
		if i&1 != 0 {
			primary.Write([]byte{0})
		} else {
			primary.Write(pw[:1])
		}
	}

	sum := primary.Sum(nil)

	// 1000 strengthening rounds, each permuting which of password/salt/previous
	// digest are mixed in and in what order.
	for i := 0; i < 1000; i++ {
		c := md5.New()

		if i&1 != 0 {
			c.Write(pw)
		} else {
			c.Write(sum[:16])
		}

		if i%3 != 0 {
			c.Write([]byte(salt))
		}

		if i%7 != 0 {
			c.Write(pw)
		}

		if i&1 != 0 {
			c.Write(sum[:16])
		} else {
			c.Write(pw)
		}

		sum = c.Sum(nil)
	}

	var out []byte

	out = to64(out, uint32(sum[0])<<16|uint32(sum[6])<<8|uint32(sum[12]), 4)
	out = to64(out, uint32(sum[1])<<16|uint32(sum[7])<<8|uint32(sum[13]), 4)
	out = to64(out, uint32(sum[2])<<16|uint32(sum[8])<<8|uint32(sum[14]), 4)
	out = to64(out, uint32(sum[3])<<16|uint32(sum[9])<<8|uint32(sum[15]), 4)
	out = to64(out, uint32(sum[4])<<16|uint32(sum[10])<<8|uint32(sum[5]), 4)
	out = to64(out, uint32(sum[11]), 2)

	return magic + salt + "$" + string(out)
}

// parseMD5Crypt splits a "<magic><salt>$<checksum>" string into its magic and
// salt so a candidate password can be re-hashed with the same parameters.
func parseMD5Crypt(hash string) (string, string, bool) {
	var magic string

	switch {
	case strings.HasPrefix(hash, magicAPR1):
		magic = magicAPR1
	case strings.HasPrefix(hash, magicMD5):
		magic = magicMD5
	default:
		return "", "", false
	}

	salt, _, found := strings.Cut(hash[len(magic):], "$")
	if !found {
		return "", "", false
	}

	if len(salt) > 8 {
		salt = salt[:8]
	}

	return magic, salt, true
}
