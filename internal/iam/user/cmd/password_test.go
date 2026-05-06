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

package user

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestResolvePasswordMode(t *testing.T) {
	// validHash is a real bcrypt hash so the hash-mode arm validates correctly.
	validHashBytes, err := bcrypt.GenerateFromPassword([]byte("password"), bcryptCost)
	require.NoError(t, err)
	validHash := string(validHashBytes)

	tests := []struct {
		name     string
		prompt   bool
		stdin    bool
		generate bool
		hash     string
		want     passwordMode
		wantErr  string
	}{
		{name: "prompt explicit", prompt: true, want: passwordModePrompt},
		{name: "stdin explicit", stdin: true, want: passwordModeStdin},
		{name: "generate explicit", generate: true, want: passwordModeGenerate},
		{name: "hash explicit", hash: validHash, want: passwordModeHash},
		{name: "prompt+stdin conflict", prompt: true, stdin: true, wantErr: "only one of"},
		{name: "prompt+generate conflict", prompt: true, generate: true, wantErr: "only one of"},
		{name: "stdin+generate conflict", stdin: true, generate: true, wantErr: "only one of"},
		{name: "hash+stdin conflict", hash: validHash, stdin: true, wantErr: "only one of"},
		{name: "hash+generate conflict", hash: validHash, generate: true, wantErr: "only one of"},
		{name: "all four conflict", prompt: true, stdin: true, generate: true, hash: validHash, wantErr: "only one of"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolvePasswordMode(tt.prompt, tt.stdin, tt.generate, tt.hash)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestReadPasswordStdin(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string
	}{
		{name: "normal", input: "mysecret\n", want: "mysecret"},
		{name: "no trailing newline", input: "mysecret", want: "mysecret"},
		{name: "empty", input: "\n", wantErr: "password must not be empty"},
		{name: "empty eof", input: "", wantErr: "no password provided"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readPasswordStdin(strings.NewReader(tt.input))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestGeneratePassword(t *testing.T) {
	pw, err := generatePassword()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(pw), 20, "generated password should be at least 20 chars")

	pw2, err := generatePassword()
	require.NoError(t, err)
	assert.NotEqual(t, pw, pw2, "two generated passwords should differ")
}

func TestEncodePasswordForDeckhouse(t *testing.T) {
	plain := "testpassword123"
	encoded, err := encodePasswordForDeckhouse(plain)
	require.NoError(t, err)
	assert.NotEmpty(t, encoded)

	// Decode from base64
	hashBytes, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)

	// Verify bcrypt hash matches the original password
	err = bcrypt.CompareHashAndPassword(hashBytes, []byte(plain))
	assert.NoError(t, err, "bcrypt hash should match the original password")

	// Verify bcrypt cost
	cost, err := bcrypt.Cost(hashBytes)
	require.NoError(t, err)
	assert.Equal(t, bcryptCost, cost)
}

func TestValidateBcryptHash(t *testing.T) {
	validBytes, err := bcrypt.GenerateFromPassword([]byte("pw"), bcryptCost)
	require.NoError(t, err)
	valid := string(validBytes)

	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "valid bcrypt", input: valid},
		{name: "empty", input: "", wantErr: "does not look like a bcrypt hash"},
		{name: "non-bcrypt prefix", input: "not-a-hash", wantErr: "does not look like a bcrypt hash"},
		{name: "wrong $1 prefix", input: "$1$abc$def", wantErr: "does not look like a bcrypt hash"},
		{name: "$2 prefix but malformed", input: "$2y$malformed", wantErr: "not a valid bcrypt hash"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBcryptHash(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPasswordResult_RawBcryptHash(t *testing.T) {
	t.Run("hash mode passes through", func(t *testing.T) {
		validBytes, err := bcrypt.GenerateFromPassword([]byte("pw"), bcryptCost)
		require.NoError(t, err)
		valid := string(validBytes)

		raw, err := passwordResult{Hash: valid}.rawBcryptHash()
		require.NoError(t, err)
		assert.Equal(t, valid, raw, "hash-mode result must return the raw hash unchanged")
	})

	t.Run("plain mode hashes once", func(t *testing.T) {
		plain := "secret123"
		raw, err := passwordResult{Plain: plain}.rawBcryptHash()
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(raw, "$2"), "raw hash should start with bcrypt prefix")

		// And must verify against the original plaintext.
		assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(raw), []byte(plain)))
	})
}

func TestEncodePasswordForUserCR_DoesNotDoubleBase64(t *testing.T) {
	// User.spec.password expects base64(<raw bcrypt hash>). The CLI must not
	// double-wrap the value (would produce base64(base64(hash))) — that would
	// silently round-trip through the apiserver but fail bcrypt verification
	// in the user-authn hook, way after CR creation.
	rawBytes, err := bcrypt.GenerateFromPassword([]byte("pw"), bcryptCost)
	require.NoError(t, err)
	raw := string(rawBytes)

	encoded := encodePasswordForUserCR(raw)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	assert.Equal(t, raw, string(decoded), "exactly one base64 wrap expected")
}
