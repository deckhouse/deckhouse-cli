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

package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestRejectFlagsInToMode(t *testing.T) {
	tests := []struct {
		name        string
		setFlag     string
		setValue    string
		wantErr     bool
		wantHintSub string
	}{
		{name: "no conflicting flags", wantErr: false},
		{
			name:        "access-level conflicts",
			setFlag:     "access-level",
			setValue:    "Admin",
			wantErr:     true,
			wantHintSub: "kubectl edit",
		},
		{
			name:        "port-forwarding conflicts with per-subject hint",
			setFlag:     "port-forwarding",
			setValue:    "true",
			wantErr:     true,
			wantHintSub: "separate grant without --to",
		},
		{
			name:        "allow-scale conflicts with shared-rule warning",
			setFlag:     "allow-scale",
			setValue:    "true",
			wantErr:     true,
			wantHintSub: "affect every subject already on it",
		},
		{
			name:        "dry-run is not supported",
			setFlag:     "dry-run",
			setValue:    "true",
			wantErr:     true,
			wantHintSub: "--dry-run is not supported",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newGrantCommand()
			if tc.setFlag != "" {
				require.NoError(t, cmd.Flags().Set(tc.setFlag, tc.setValue))
			}

			err := rejectFlagsInToMode(cmd)
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not allowed with --to")
			if tc.wantHintSub != "" {
				assert.Contains(t, err.Error(), tc.wantHintSub)
			}
		})
	}
}

func TestAddSubject(t *testing.T) {
	build := func(subjects []any) *unstructured.Unstructured {
		return &unstructured.Unstructured{
			Object: map[string]any{
				"spec": map[string]any{"subjects": subjects},
			},
		}
	}

	t.Run("adds to empty subjects", func(t *testing.T) {
		obj := build(nil)
		newSubs, added := addSubject(obj, "User", "alice@example.com")
		assert.True(t, added)
		assert.Len(t, newSubs, 1)
	})

	t.Run("appends when principal is new", func(t *testing.T) {
		obj := build([]any{
			map[string]any{"kind": "User", "name": "bob@example.com"},
		})
		newSubs, added := addSubject(obj, "User", "alice@example.com")
		assert.True(t, added)
		assert.Len(t, newSubs, 2)
	})

	t.Run("is idempotent for exact duplicate", func(t *testing.T) {
		obj := build([]any{
			map[string]any{"kind": "User", "name": "alice@example.com"},
		})
		newSubs, added := addSubject(obj, "User", "alice@example.com")
		assert.False(t, added)
		assert.Len(t, newSubs, 1)
	})

	t.Run("kind is part of identity", func(t *testing.T) {
		// Group "alice" and User "alice" are not the same subject.
		obj := build([]any{
			map[string]any{"kind": "Group", "name": "alice"},
		})
		newSubs, added := addSubject(obj, "User", "alice")
		assert.True(t, added)
		assert.Len(t, newSubs, 2)
	})
}
