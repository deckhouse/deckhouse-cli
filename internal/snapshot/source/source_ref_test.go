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

package source_test

import (
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

func TestParseSourceRef(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		raw       string
		wantName  string
		wantKind  string
		wantNS    string
		wantUID   string
		wantError bool
	}{
		{
			name:     "valid full annotation",
			raw:      `{"apiVersion":"storage.deckhouse.io/v1alpha1","kind":"VirtualMachine","namespace":"ns-a","name":"my-vm","uid":"uid-abc-123"}`,
			wantName: "my-vm",
			wantKind: "VirtualMachine",
			wantNS:   "ns-a",
			wantUID:  "uid-abc-123",
		},
		{
			name:     "valid PVC annotation",
			raw:      `{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"default","name":"my-pvc","uid":"pvc-uid-999"}`,
			wantName: "my-pvc",
			wantKind: "PersistentVolumeClaim",
			wantNS:   "default",
			wantUID:  "pvc-uid-999",
		},
		{
			name:      "empty annotation",
			raw:       "",
			wantError: true,
		},
		{
			name:      "malformed JSON",
			raw:       `not-json`,
			wantError: true,
		},
		{
			name:      "truncated JSON",
			raw:       `{"apiVersion":"v1","kind":`,
			wantError: true,
		},
		{
			name:     "JSON with only name",
			raw:      `{"name":"just-name"}`,
			wantName: "just-name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := source.ParseSourceRef(tc.raw)

			if tc.wantError {
				if err == nil {
					t.Errorf("expected error, got nil; result: %+v", got)
				}

				// On error the zero value must be returned.
				if got != (source.SourceRefIdentity{}) {
					t.Errorf("on error expected zero SourceRefIdentity, got %+v", got)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got.Name != tc.wantName {
				t.Errorf("Name: got %q, want %q", got.Name, tc.wantName)
			}

			if tc.wantKind != "" && got.Kind != tc.wantKind {
				t.Errorf("Kind: got %q, want %q", got.Kind, tc.wantKind)
			}

			if tc.wantNS != "" && got.Namespace != tc.wantNS {
				t.Errorf("Namespace: got %q, want %q", got.Namespace, tc.wantNS)
			}

			if tc.wantUID != "" && got.UID != tc.wantUID {
				t.Errorf("UID: got %q, want %q", got.UID, tc.wantUID)
			}
		})
	}
}
