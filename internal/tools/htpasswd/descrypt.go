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

// Traditional Unix DES crypt(3): the legacy 13-character hash ('htpasswd -d').
// It is a modified DES that encrypts a zero block 25 times, with the 12-bit
// salt perturbing the expansion permutation. Insecure (8-char password limit,
// trivially brute-forced) and provided only for htpasswd parity.
//
// The tables below are the standard FIPS-46 DES permutations and S-boxes; the
// algorithm works on bits as 0/1 bytes for clarity rather than speed — a CLI
// hashing one password does not need the packed-word optimizations.

var desIP = [64]int{
	58, 50, 42, 34, 26, 18, 10, 2, 60, 52, 44, 36, 28, 20, 12, 4,
	62, 54, 46, 38, 30, 22, 14, 6, 64, 56, 48, 40, 32, 24, 16, 8,
	57, 49, 41, 33, 25, 17, 9, 1, 59, 51, 43, 35, 27, 19, 11, 3,
	61, 53, 45, 37, 29, 21, 13, 5, 63, 55, 47, 39, 31, 23, 15, 7,
}

var desFP = [64]int{
	40, 8, 48, 16, 56, 24, 64, 32, 39, 7, 47, 15, 55, 23, 63, 31,
	38, 6, 46, 14, 54, 22, 62, 30, 37, 5, 45, 13, 53, 21, 61, 29,
	36, 4, 44, 12, 52, 20, 60, 28, 35, 3, 43, 11, 51, 19, 59, 27,
	34, 2, 42, 10, 50, 18, 58, 26, 33, 1, 41, 9, 49, 17, 57, 25,
}

var desE = [48]int{
	32, 1, 2, 3, 4, 5, 4, 5, 6, 7, 8, 9,
	8, 9, 10, 11, 12, 13, 12, 13, 14, 15, 16, 17,
	16, 17, 18, 19, 20, 21, 20, 21, 22, 23, 24, 25,
	24, 25, 26, 27, 28, 29, 28, 29, 30, 31, 32, 1,
}

var desP = [32]int{
	16, 7, 20, 21, 29, 12, 28, 17, 1, 15, 23, 26, 5, 18, 31, 10,
	2, 8, 24, 14, 32, 27, 3, 9, 19, 13, 30, 6, 22, 11, 4, 25,
}

var desPC1 = [56]int{
	57, 49, 41, 33, 25, 17, 9, 1, 58, 50, 42, 34, 26, 18,
	10, 2, 59, 51, 43, 35, 27, 19, 11, 3, 60, 52, 44, 36,
	63, 55, 47, 39, 31, 23, 15, 7, 62, 54, 46, 38, 30, 22,
	14, 6, 61, 53, 45, 37, 29, 21, 13, 5, 28, 20, 12, 4,
}

var desPC2 = [48]int{
	14, 17, 11, 24, 1, 5, 3, 28, 15, 6, 21, 10,
	23, 19, 12, 4, 26, 8, 16, 7, 27, 20, 13, 2,
	41, 52, 31, 37, 47, 55, 30, 40, 51, 45, 33, 48,
	44, 49, 39, 56, 34, 53, 46, 42, 50, 36, 29, 32,
}

var desShifts = [16]int{1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1}

var desS = [8][64]int{
	{
		14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7,
		0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8,
		4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0,
		15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13,
	},
	{
		15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10,
		3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5,
		0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15,
		13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9,
	},
	{
		10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8,
		13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1,
		13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7,
		1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12,
	},
	{
		7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15,
		13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9,
		10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4,
		3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14,
	},
	{
		2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9,
		14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6,
		4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14,
		11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3,
	},
	{
		12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11,
		10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8,
		9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6,
		4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13,
	},
	{
		4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1,
		13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6,
		1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2,
		6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12,
	},
	{
		13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7,
		1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2,
		7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8,
		2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11,
	},
}

// desSubkeys derives the 16 round subkeys (each 48 bits, as 0/1 bytes) from an
// 8-byte key block (also expressed as 64 0/1 bits).
func desSubkeys(keyBits []byte) [16][48]byte {
	// PC1: 64 -> 56 bits, split into halves c and d.
	var c, d [28]byte
	for i := 0; i < 28; i++ {
		c[i] = keyBits[desPC1[i]-1]
		d[i] = keyBits[desPC1[i+28]-1]
	}

	var subkeys [16][48]byte

	for round := 0; round < 16; round++ {
		c = rotl28(c, desShifts[round])
		d = rotl28(d, desShifts[round])

		// PC2: pick 48 bits out of the concatenated 56-bit c||d.
		for i := 0; i < 48; i++ {
			pos := desPC2[i] - 1
			if pos < 28 {
				subkeys[round][i] = c[pos]
			} else {
				subkeys[round][i] = d[pos-28]
			}
		}
	}

	return subkeys
}

func rotl28(in [28]byte, n int) [28]byte {
	var out [28]byte
	for i := 0; i < 28; i++ {
		out[i] = in[(i+n)%28]
	}

	return out
}

// desEncryptZero encrypts the all-zero block with the given subkeys, applying
// the salt perturbation to the expansion each round, and returns the 64-bit
// output as 0/1 bytes. crypt(3) iterates this 25 times.
func desEncryptZero(block [64]byte, subkeys [16][48]byte, salt uint32) [64]byte {
	// Initial permutation.
	var perm [64]byte
	for i := 0; i < 64; i++ {
		perm[i] = block[desIP[i]-1]
	}

	var left, right [32]byte
	copy(left[:], perm[:32])
	copy(right[:], perm[32:])

	for round := 0; round < 16; round++ {
		f := desFeistel(right, subkeys[round], salt)

		var next [32]byte
		for i := 0; i < 32; i++ {
			next[i] = left[i] ^ f[i]
		}

		left = right
		right = next
	}

	// Preoutput is right||left (halves swapped), then the final permutation.
	var pre [64]byte
	copy(pre[:32], right[:])
	copy(pre[32:], left[:])

	var out [64]byte
	for i := 0; i < 64; i++ {
		out[i] = pre[desFP[i]-1]
	}

	return out
}

// desFeistel is the DES round function f(R, K) with the crypt(3) salt twist:
// after expanding R to 48 bits, bit i is swapped with bit i+24 when bit i of
// the salt is set (i in 0..11).
func desFeistel(right [32]byte, subkey [48]byte, salt uint32) [32]byte {
	var expanded [48]byte
	for i := 0; i < 48; i++ {
		expanded[i] = right[desE[i]-1]
	}

	for i := 0; i < 24; i++ {
		if salt&(1<<uint(i)) != 0 {
			expanded[i], expanded[i+24] = expanded[i+24], expanded[i]
		}
	}

	for i := 0; i < 48; i++ {
		expanded[i] ^= subkey[i]
	}

	// Eight S-boxes: 6 bits in, 4 bits out.
	var sout [32]byte

	for box := 0; box < 8; box++ {
		b := expanded[box*6 : box*6+6]
		row := int(b[0])<<1 | int(b[5])
		col := int(b[1])<<3 | int(b[2])<<2 | int(b[3])<<1 | int(b[4])
		val := desS[box][row*16+col]

		for bit := 0; bit < 4; bit++ {
			sout[box*4+bit] = byte((val >> uint(3-bit)) & 1)
		}
	}

	var out [32]byte
	for i := 0; i < 32; i++ {
		out[i] = sout[desP[i]-1]
	}

	return out
}

// desCrypt implements the classic crypt(3): a 2-character salt followed by 11
// characters encoding the 64-bit result. Only the first 8 bytes of the password
// contribute; each contributes its low 7 bits.
func desCrypt(password, salt string) string {
	for len(salt) < 2 {
		salt += "."
	}

	salt = salt[:2]

	// Decode the 12-bit salt from its two crypt64 characters.
	var saltVal uint32
	for i := 1; i >= 0; i-- {
		saltVal = saltVal<<6 | uint32(crypt64Index(salt[i]))
	}

	// Build the 64-bit key: 8 bytes, each password char's low 7 bits shifted
	// left by one (the low parity bit is left zero).
	var keyBits [64]byte

	for j := 0; j < 8; j++ {
		var ch byte
		if j < len(password) {
			ch = password[j]
		}

		kb := (ch & 0x7f) << 1
		for bit := 0; bit < 8; bit++ {
			keyBits[j*8+bit] = (kb >> uint(7-bit)) & 1
		}
	}

	subkeys := desSubkeys(keyBits[:])

	var block [64]byte
	for i := 0; i < 25; i++ {
		block = desEncryptZero(block, subkeys, saltVal)
	}

	// Encode the 64 output bits into 11 crypt64 chars, big-endian, zero-padded
	// on the final (2-bit) group.
	out := make([]byte, 0, 11)
	pos := 0

	for group := 0; group < 11; group++ {
		var v int
		for k := 0; k < 6; k++ {
			v <<= 1
			if pos < 64 {
				v |= int(block[pos])
			}

			pos++
		}

		out = append(out, crypt64[v])
	}

	return salt + string(out)
}

// crypt64Index returns the value of a crypt64 character, or 0 if it is not in
// the alphabet.
func crypt64Index(c byte) int {
	for i := 0; i < len(crypt64); i++ {
		if crypt64[i] == c {
			return i
		}
	}

	return 0
}
