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
	"crypto/rand"
	"fmt"
)

// crypt64 is the non-standard base64 alphabet shared by the crypt(3) family
// (DES, MD5/apr1, SHA-256/512). Note the ordering: './0-9A-Za-z', which differs
// from RFC 4648 and must not be swapped for encoding/base64.
const crypt64 = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// to64 appends n crypt64 characters encoding the low 6*n bits of v, least
// significant group first. This is the byte order used by the MD5 and SHA
// crypt output assembly.
func to64(dst []byte, v uint32, n int) []byte {
	for ; n > 0; n-- {
		dst = append(dst, crypt64[v&0x3f])
		v >>= 6
	}

	return dst
}

// randomSalt returns n random characters drawn from the crypt64 alphabet, used
// to seed a fresh hash. It draws from crypto/rand so salts are unpredictable.
func randomSalt(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating salt: %w", err)
	}

	for i := range buf {
		buf[i] = crypt64[int(buf[i])%len(crypt64)]
	}

	return string(buf), nil
}
