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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func TestValidateUserName(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantValid bool
	}{
		{name: "valid simple", input: "anton", wantValid: true},
		{name: "valid with dash", input: "anton-user", wantValid: true},
		{name: "valid with numbers", input: "user123", wantValid: true},
		{name: "empty", input: ""},
		{name: "uppercase", input: "Anton"},
		{name: "underscore", input: "my_user"},
		{name: "starts with dash", input: "-user"},
		{name: "ends with dash", input: "user-"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUserName(tt.input)
			if tt.wantValid {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid user name")
			}
		})
	}
}

func TestValidateEmail(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "valid", input: "anton@abc.com"},
		{name: "empty", input: "", wantErr: "--email is required"},
		{name: "uppercase", input: "Anton@abc.com", wantErr: "must be lowercase"},
		{name: "no at sign", input: "antonabc.com", wantErr: "does not look like a valid email"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEmail(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateTTL(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "hours", input: "24h"},
		{name: "minutes", input: "30m"},
		{name: "hours and minutes", input: "1h30m"},
		{name: "seconds", input: "30s"},
		{name: "invalid format", input: "2d", wantErr: "invalid --ttl"},
		{name: "negative", input: "-1h", wantErr: "must be positive"},
		{name: "zero", input: "0s", wantErr: "must be positive"},
		{name: "empty", input: "", wantErr: "invalid --ttl"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTTL(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBuildUserObject(t *testing.T) {
	t.Run("without ttl", func(t *testing.T) {
		obj := buildUserObject("anton", "anton@abc.com", "encodedpass", "")
		assert.Equal(t, "deckhouse.io/v1", obj.GetAPIVersion())
		assert.Equal(t, "User", obj.GetKind())
		assert.Equal(t, "anton", obj.GetName())

		email, _, _ := unstructured.NestedString(obj.Object, "spec", "email")
		assert.Equal(t, "anton@abc.com", email)

		password, _, _ := unstructured.NestedString(obj.Object, "spec", "password")
		assert.Equal(t, "encodedpass", password)

		_, found, _ := unstructured.NestedString(obj.Object, "spec", "ttl")
		assert.False(t, found)
	})

	t.Run("with ttl", func(t *testing.T) {
		obj := buildUserObject("anton", "anton@abc.com", "encodedpass", "24h")
		ttl, found, _ := unstructured.NestedString(obj.Object, "spec", "ttl")
		assert.True(t, found)
		assert.Equal(t, "24h", ttl)
	})
}

func TestBuildUserObject_DryRunYAML(t *testing.T) {
	obj := buildUserObject("test-user", "test@example.com", "aGFzaA==", "")

	var buf = &strings.Builder{}
	err := utilk8s.PrintObject(buf, obj, "yaml")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "kind: User")
	assert.Contains(t, output, "name: test-user")
	assert.Contains(t, output, "email: test@example.com")
}

func TestBuildUserObject_DryRunJSON(t *testing.T) {
	obj := buildUserObject("test-user", "test@example.com", "aGFzaA==", "")

	var buf = &strings.Builder{}
	err := utilk8s.PrintObject(buf, obj, "json")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, `"kind": "User"`)
	assert.Contains(t, output, `"name": "test-user"`)
}
