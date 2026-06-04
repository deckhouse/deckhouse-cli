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

package source

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeTopLevelArray(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantN   int
		wantErr bool
	}{
		{
			name:  "empty array",
			input: `[]`,
			wantN: 0,
		},
		{
			name:  "two objects",
			input: `[{"kind":"ConfigMap"},{"kind":"Deployment"}]`,
			wantN: 2,
		},
		{
			name:    "not an array",
			input:   `{"kind":"ConfigMap"}`,
			wantErr: true,
		},
		{
			name:    "truncated JSON",
			input:   `[{"kind":"ConfigMap"`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			items, err := decodeTopLevelArray(strings.NewReader(tc.input))
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(items) != tc.wantN {
				t.Fatalf("got %d items, want %d", len(items), tc.wantN)
			}

			for i, item := range items {
				var v any

				if err := json.Unmarshal(item, &v); err != nil {
					t.Fatalf("item[%d] is invalid JSON: %v", i, err)
				}
			}
		})
	}
}

func TestParseObjectFlag(t *testing.T) {
	tests := []struct {
		name     string
		flag     string
		wantAPI  string
		wantKind string
		wantName string
		wantErr  bool
	}{
		{
			name:     "apps/v1/Deployment/my-deploy",
			flag:     "apps/v1/Deployment/my-deploy",
			wantAPI:  "apps/v1",
			wantKind: "Deployment",
			wantName: "my-deploy",
		},
		{
			name:     "core v1 ConfigMap",
			flag:     "v1/ConfigMap/my-cm",
			wantAPI:  "v1",
			wantKind: "ConfigMap",
			wantName: "my-cm",
		},
		{
			name:    "too few segments",
			flag:    "Deployment/my-deploy",
			wantErr: true,
		},
		{
			name:    "empty string",
			flag:    "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api, kind, name, err := parseObjectFlag(tc.flag)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if api != tc.wantAPI {
				t.Fatalf("apiVersion = %q, want %q", api, tc.wantAPI)
			}

			if kind != tc.wantKind {
				t.Fatalf("kind = %q, want %q", kind, tc.wantKind)
			}

			if name != tc.wantName {
				t.Fatalf("name = %q, want %q", name, tc.wantName)
			}
		})
	}
}

func TestBuildObjectFilter(t *testing.T) {
	cm := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"my-cm"}}`)
	deploy := []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"my-deploy"}}`)

	filter, err := BuildObjectFilter("v1/ConfigMap/my-cm")
	if err != nil {
		t.Fatalf("BuildObjectFilter: %v", err)
	}

	keepCM, err := filter(cm)
	if err != nil {
		t.Fatalf("filter(cm): %v", err)
	}

	if !keepCM {
		t.Fatal("expected filter to keep ConfigMap")
	}

	keepDeploy, err := filter(deploy)
	if err != nil {
		t.Fatalf("filter(deploy): %v", err)
	}

	if keepDeploy {
		t.Fatal("expected filter to drop Deployment")
	}
}

func TestBuildObjectFilter_EmptyFlag(t *testing.T) {
	filter, err := BuildObjectFilter("")
	if err != nil {
		t.Fatalf("BuildObjectFilter empty: %v", err)
	}

	if filter != nil {
		t.Fatal("expected nil filter for empty flag")
	}
}
