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
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"strconv"
	"strings"
)

// SHA-crypt parameters, matching glibc: rounds default to 5000 and are clamped
// to [1000, 999999999]. The 'rounds=' field is emitted only when it differs
// from the default, so common hashes stay compact.
const (
	shaCryptDefaultRounds = 5000
	shaCryptMinRounds     = 1000
	shaCryptMaxRounds     = 999999999
)

// shaCrypt implements Ulrich Drepper's SHA-256/512 crypt (the '$5$' and '$6$'
// schemes). is512 selects SHA-512 (64-byte digest) over SHA-256 (32-byte).
// Salt is capped at 16 characters. See https://www.akkadia.org/drepper/SHA-crypt.txt.
func shaCrypt(password, salt string, rounds int, is512 bool) string {
	if len(salt) > 16 {
		salt = salt[:16]
	}

	explicitRounds := rounds != 0
	if rounds == 0 {
		rounds = shaCryptDefaultRounds
	}

	if rounds < shaCryptMinRounds {
		rounds = shaCryptMinRounds
	}

	if rounds > shaCryptMaxRounds {
		rounds = shaCryptMaxRounds
	}

	newHash := func() hash.Hash {
		if is512 {
			return sha512.New()
		}

		return sha256.New()
	}

	var size int
	if is512 {
		size = sha512.Size
	} else {
		size = sha256.Size
	}

	pw := []byte(password)
	saltB := []byte(salt)

	// Digest B = H(password, salt, password).
	b := newHash()
	b.Write(pw)
	b.Write(saltB)
	b.Write(pw)
	sumB := b.Sum(nil)

	// Digest A = H(password, salt, B repeated for len(password), then a bit
	// pattern of A/password chosen by the bits of len(password)).
	a := newHash()
	a.Write(pw)
	a.Write(saltB)

	for i := len(pw); i > 0; i -= size {
		if i > size {
			a.Write(sumB)
		} else {
			a.Write(sumB[:i])
		}
	}

	for i := len(pw); i > 0; i >>= 1 {
		if i&1 != 0 {
			a.Write(sumB)
		} else {
			a.Write(pw)
		}
	}

	sumA := a.Sum(nil)

	// Sequence P: H(password)*len(password), tiled to len(password) bytes.
	dp := newHash()
	for i := 0; i < len(pw); i++ {
		dp.Write(pw)
	}

	sumDP := dp.Sum(nil)
	p := tile(sumDP, len(pw), size)

	// Sequence S: H(salt)*(16 + A[0]), tiled to len(salt) bytes.
	ds := newHash()
	for i := 0; i < 16+int(sumA[0]); i++ {
		ds.Write(saltB)
	}

	sumDS := ds.Sum(nil)
	s := tile(sumDS, len(saltB), size)

	// Strengthening loop: `rounds` iterations, each mixing P, S and the running
	// digest in an order chosen by the round index.
	cur := sumA

	for i := 0; i < rounds; i++ {
		c := newHash()

		if i&1 != 0 {
			c.Write(p)
		} else {
			c.Write(cur)
		}

		if i%3 != 0 {
			c.Write(s)
		}

		if i%7 != 0 {
			c.Write(p)
		}

		if i&1 != 0 {
			c.Write(cur)
		} else {
			c.Write(p)
		}

		cur = c.Sum(nil)
	}

	checksum := shaCryptEncode(cur, is512)

	var prefix string
	if is512 {
		prefix = "$6$"
	} else {
		prefix = "$5$"
	}

	if explicitRounds || rounds != shaCryptDefaultRounds {
		prefix += "rounds=" + strconv.Itoa(rounds) + "$"
	}

	return prefix + salt + "$" + checksum
}

// tile repeats src to fill exactly length bytes (whole copies of size, then a
// final partial copy), producing the P and S sequences.
func tile(src []byte, length, size int) []byte {
	out := make([]byte, 0, length)
	for length >= size {
		out = append(out, src...)
		length -= size
	}

	return append(out, src[:length]...)
}

// shaCryptEncode performs the scheme-specific reordering of the final digest
// into crypt64 characters. The index permutation differs between SHA-256 and
// SHA-512 and comes straight from Drepper's reference implementation.
func shaCryptEncode(d []byte, is512 bool) string {
	var out []byte

	if is512 {
		groups := [][3]int{
			{0, 21, 42}, {22, 43, 1}, {44, 2, 23}, {3, 24, 45}, {25, 46, 4},
			{47, 5, 26}, {6, 27, 48}, {28, 49, 7}, {50, 8, 29}, {9, 30, 51},
			{31, 52, 10}, {53, 11, 32}, {12, 33, 54}, {34, 55, 13}, {56, 14, 35},
			{15, 36, 57}, {37, 58, 16}, {59, 17, 38}, {18, 39, 60}, {40, 61, 19},
			{62, 20, 41},
		}
		for _, g := range groups {
			out = to64(out, uint32(d[g[0]])<<16|uint32(d[g[1]])<<8|uint32(d[g[2]]), 4)
		}

		out = to64(out, uint32(d[63]), 2)

		return string(out)
	}

	groups := [][3]int{
		{0, 10, 20}, {21, 1, 11}, {12, 22, 2}, {3, 13, 23}, {24, 4, 14},
		{15, 25, 5}, {6, 16, 26}, {27, 7, 17}, {18, 28, 8}, {9, 19, 29},
	}
	for _, g := range groups {
		out = to64(out, uint32(d[g[0]])<<16|uint32(d[g[1]])<<8|uint32(d[g[2]]), 4)
	}

	out = to64(out, uint32(d[31])<<8|uint32(d[30]), 3)

	return string(out)
}

// parseShaCrypt extracts the rounds and salt from a '$5$'/'$6$' hash so a
// candidate password can be re-hashed identically for verification.
func parseShaCrypt(hash string) (int, string, bool, bool) {
	is512 := strings.HasPrefix(hash, "$6$")
	if !is512 && !strings.HasPrefix(hash, "$5$") {
		return 0, "", false, false
	}

	// fields: ["", "5"|"6", (optional "rounds=N"), salt, checksum]
	fields := strings.Split(hash, "$")[2:]

	rounds := 0

	if len(fields) > 0 && strings.HasPrefix(fields[0], "rounds=") {
		n, err := strconv.Atoi(strings.TrimPrefix(fields[0], "rounds="))
		if err != nil {
			return 0, "", false, false
		}

		rounds = n
		fields = fields[1:]
	}

	if len(fields) < 1 {
		return 0, "", false, false
	}

	salt := fields[0]
	if len(salt) > 16 {
		salt = salt[:16]
	}

	return rounds, salt, is512, true
}
