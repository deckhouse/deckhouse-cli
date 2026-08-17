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
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// algorithm identifies one of htpasswd's password schemes.
type algorithm string

const (
	algBcrypt algorithm = "bcrypt" // -B, $2y$
	algAPR1   algorithm = "md5"    // -m, $apr1$
	algSHA256 algorithm = "sha256" // -2, $5$
	algSHA512 algorithm = "sha512" // -5, $6$
	algDES    algorithm = "crypt"  // -d, legacy 13-char
	algSHA1   algorithm = "sha1"   // -s, {SHA}
	algPlain  algorithm = "plain"  // -p, verbatim
)

// hashOptions carries the algorithm selection plus its tunable parameters.
type hashOptions struct {
	alg    algorithm
	cost   int // bcrypt work factor
	rounds int // SHA-256/512 rounds; 0 means the scheme default
}

// generateHash hashes password with a fresh random salt appropriate to the
// selected algorithm.
func generateHash(password string, o hashOptions) (string, error) {
	switch o.alg {
	case algBcrypt:
		return bcryptHash(password, o.cost)
	case algAPR1:
		salt, err := randomSalt(8)
		if err != nil {
			return "", err
		}

		return md5Crypt(password, salt, magicAPR1), nil
	case algSHA256:
		salt, err := randomSalt(16)
		if err != nil {
			return "", err
		}

		return shaCrypt(password, salt, o.rounds, false), nil
	case algSHA512:
		salt, err := randomSalt(16)
		if err != nil {
			return "", err
		}

		return shaCrypt(password, salt, o.rounds, true), nil
	case algDES:
		salt, err := randomSalt(2)
		if err != nil {
			return "", err
		}

		return desCrypt(password, salt), nil
	case algSHA1:
		return sha1Hash(password), nil
	case algPlain:
		return password, nil
	}

	return "", fmt.Errorf("unknown algorithm %q", o.alg)
}

// bcryptHash returns a bcrypt hash rewritten to the '$2y$' identifier that
// Apache htpasswd emits. Go's bcrypt produces the equivalent '$2a$' and can
// verify either, so the rewrite is purely cosmetic parity.
func bcryptHash(password string, cost int) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return "", fmt.Errorf("hashing password: %w", err)
	}

	return "$2y$" + string(h[4:]), nil
}

// sha1Hash returns the '{SHA}' scheme: base64 of the unsalted SHA-1 digest.
func sha1Hash(password string) string {
	sum := sha1.Sum([]byte(password))

	return "{SHA}" + base64.StdEncoding.EncodeToString(sum[:])
}

// verifyHash reports whether password matches an existing hash, auto-detecting
// the scheme from the stored value. Unknown-prefix values are tried as DES and
// then as plaintext, mirroring how htpasswd -v probes a password file.
func verifyHash(password, stored string) (bool, error) {
	switch {
	case strings.HasPrefix(stored, "$2a$"), strings.HasPrefix(stored, "$2b$"), strings.HasPrefix(stored, "$2y$"):
		err := bcrypt.CompareHashAndPassword([]byte(stored), []byte(password))
		if err == nil {
			return true, nil
		}

		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return false, nil
		}

		return false, err
	case strings.HasPrefix(stored, magicAPR1), strings.HasPrefix(stored, magicMD5):
		magic, salt, ok := parseMD5Crypt(stored)
		if !ok {
			return false, fmt.Errorf("malformed MD5 hash")
		}

		return constEq(md5Crypt(password, salt, magic), stored), nil
	case strings.HasPrefix(stored, "$5$"), strings.HasPrefix(stored, "$6$"):
		rounds, salt, is512, ok := parseShaCrypt(stored)
		if !ok {
			return false, fmt.Errorf("malformed SHA-crypt hash")
		}

		return constEq(shaCrypt(password, salt, rounds, is512), stored), nil
	case strings.HasPrefix(stored, "{SHA}"):
		return constEq(sha1Hash(password), stored), nil
	}

	// No recognizable prefix. A 13-character crypt64 string is almost certainly
	// DES crypt; anything else can only be a plaintext ('-p') entry.
	if len(stored) == 13 && isCrypt64(stored) && constEq(desCrypt(password, stored[:2]), stored) {
		return true, nil
	}

	return constEq(password, stored), nil
}

// constEq compares two strings in constant time (length mismatch returns false).
func constEq(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// isCrypt64 reports whether every byte of s is a member of the crypt64 alphabet.
func isCrypt64(s string) bool {
	for i := 0; i < len(s); i++ {
		if !strings.ContainsRune(crypt64, rune(s[i])) {
			return false
		}
	}

	return true
}
