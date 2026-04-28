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

package group

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

func TestBuildGroupObject(t *testing.T) {
	obj := buildGroupObject("admins")
	assert.Equal(t, "deckhouse.io/v1alpha1", obj.GetAPIVersion())
	assert.Equal(t, "Group", obj.GetKind())
	assert.Equal(t, "admins", obj.GetName())

	specName, _, _ := unstructured.NestedString(obj.Object, "spec", "name")
	assert.Equal(t, "admins", specName)

	members, found, _ := unstructured.NestedSlice(obj.Object, "spec", "members")
	assert.True(t, found)
	assert.Empty(t, members)
}

func TestParseMemberArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantGroup  string
		wantKind   string
		wantMember string
		wantErr    bool
	}{
		{name: "two args defaults to user", args: []string{"admins", "anton"}, wantGroup: "admins", wantKind: "user", wantMember: "anton"},
		{name: "three args explicit user", args: []string{"admins", "user", "anton"}, wantGroup: "admins", wantKind: "user", wantMember: "anton"},
		{name: "three args explicit group", args: []string{"platform", "group", "admins"}, wantGroup: "platform", wantKind: "group", wantMember: "admins"},
		{name: "one arg is error", args: []string{"admins"}, wantErr: true},
		{name: "four args is error", args: []string{"a", "b", "c", "d"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, k, m, err := parseMemberArgs(tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantGroup, g)
			assert.Equal(t, tt.wantKind, k)
			assert.Equal(t, tt.wantMember, m)
		})
	}
}

func TestNormalizeMemberKind(t *testing.T) {
	tests := []struct {
		input   string
		want    iamtypes.SubjectKind
		wantErr string
	}{
		{input: "user", want: iamtypes.KindUser},
		{input: "User", want: iamtypes.KindUser},
		{input: "USER", want: iamtypes.KindUser},
		{input: "group", want: iamtypes.KindGroup},
		{input: "Group", want: iamtypes.KindGroup},
		{input: "invalid", wantErr: "invalid member kind"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := normalizeMemberKind(tt.input)
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

func TestGetGroupMembers(t *testing.T) {
	t.Run("with members", func(t *testing.T) {
		obj := &unstructured.Unstructured{
			Object: map[string]any{
				"spec": map[string]any{
					"members": []any{
						map[string]any{"kind": "User", "name": "anton"},
						map[string]any{"kind": "Group", "name": "devs"},
					},
				},
			},
		}
		members, err := getGroupMembers(obj)
		require.NoError(t, err)
		assert.Len(t, members, 2)
	})

	t.Run("empty members", func(t *testing.T) {
		obj := &unstructured.Unstructured{
			Object: map[string]any{
				"spec": map[string]any{
					"members": []any{},
				},
			},
		}
		members, err := getGroupMembers(obj)
		require.NoError(t, err)
		assert.Empty(t, members)
	})

	t.Run("no members field", func(t *testing.T) {
		obj := &unstructured.Unstructured{
			Object: map[string]any{
				"spec": map[string]any{},
			},
		}
		members, err := getGroupMembers(obj)
		require.NoError(t, err)
		assert.Nil(t, members)
	})
}

