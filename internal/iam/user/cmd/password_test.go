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
	tests := []struct {
		name     string
		prompt   bool
		stdin    bool
		generate bool
		want     passwordMode
		wantErr  string
	}{
		{name: "prompt explicit", prompt: true, want: passwordModePrompt},
		{name: "stdin explicit", stdin: true, want: passwordModeStdin},
		{name: "generate explicit", generate: true, want: passwordModeGenerate},
		{name: "prompt+stdin conflict", prompt: true, stdin: true, wantErr: "only one of"},
		{name: "prompt+generate conflict", prompt: true, generate: true, wantErr: "only one of"},
		{name: "stdin+generate conflict", stdin: true, generate: true, wantErr: "only one of"},
		{name: "all three conflict", prompt: true, stdin: true, generate: true, wantErr: "only one of"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolvePasswordMode(tt.prompt, tt.stdin, tt.generate)
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
