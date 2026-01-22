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

package deckhousereleases

import (
	"testing"
)

func TestDeckhouseReleaseProcessing(t *testing.T) {
	tests := []struct {
		name   string
		item   map[string]interface{}
		want   DeckhouseRelease
		wantOk bool
	}{
		{
			name: "complete valid release",
			item: map[string]interface{}{
				"status": map[string]interface{}{
					"phase":          "Deployed",
					"transitionTime": "2025-01-15T10:30:00Z",
					"message":        "Release deployed successfully",
				},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "Deployed",
				TransitionTime: "2025-01-15T10:30:00Z",
				Message:        "Release deployed successfully",
			},
			wantOk: true,
		},
		{
			name: "release with empty optional fields",
			item: map[string]interface{}{
				"status": map[string]interface{}{
					"phase": "Pending",
				},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "Pending",
				TransitionTime: "",
				Message:        "",
			},
			wantOk: true,
		},
		{
			name: "release with partial fields",
			item: map[string]interface{}{
				"status": map[string]interface{}{
					"phase":   "Superseded",
					"message": "Superseded by newer release",
				},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "Superseded",
				TransitionTime: "",
				Message:        "Superseded by newer release",
			},
			wantOk: true,
		},
		{
			name:   "nil item",
			item:   nil,
			want:   DeckhouseRelease{},
			wantOk: false,
		},
		{
			name: "missing status field",
			item: map[string]interface{}{
				"spec": map[string]interface{}{
					"version": "1.2.3",
				},
			},
			want:   DeckhouseRelease{},
			wantOk: false,
		},
		{
			name: "status is nil",
			item: map[string]interface{}{
				"status": nil,
			},
			want:   DeckhouseRelease{},
			wantOk: false,
		},
		{
			name: "status is not a map",
			item: map[string]interface{}{
				"status": "not-a-map",
			},
			want:   DeckhouseRelease{},
			wantOk: false,
		},
		{
			name: "phase is not a string",
			item: map[string]interface{}{
				"status": map[string]interface{}{
					"phase": 123,
				},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "",
				TransitionTime: "",
				Message:        "",
			},
			wantOk: true,
		},
		{
			name: "transitionTime is not a string",
			item: map[string]interface{}{
				"status": map[string]interface{}{
					"phase":          "Deployed",
					"transitionTime": 1234567890,
				},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "Deployed",
				TransitionTime: "",
				Message:        "",
			},
			wantOk: true,
		},
		{
			name: "message is not a string",
			item: map[string]interface{}{
				"status": map[string]interface{}{
					"phase":   "Deployed",
					"message": []string{"not", "a", "string"},
				},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "Deployed",
				TransitionTime: "",
				Message:        "",
			},
			wantOk: true,
		},
		{
			name: "phase value is nil",
			item: map[string]interface{}{
				"status": map[string]interface{}{
					"phase": nil,
				},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "",
				TransitionTime: "",
				Message:        "",
			},
			wantOk: true,
		},
		{
			name: "empty status map",
			item: map[string]interface{}{
				"status": map[string]interface{}{},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "",
				TransitionTime: "",
				Message:        "",
			},
			wantOk: true,
		},
		{
			name: "all fields with different types that should be ignored",
			item: map[string]interface{}{
				"status": map[string]interface{}{
					"phase":          map[string]interface{}{"nested": "value"},
					"transitionTime": []int{1, 2, 3},
					"message":        true,
				},
			},
			want: DeckhouseRelease{
				Name:           "test-release",
				Phase:          "",
				TransitionTime: "",
				Message:        "",
			},
			wantOk: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := deckhouseReleaseProcessing(tt.item, "test-release")
			if gotOk != tt.wantOk {
				t.Errorf("deckhouseReleaseProcessing() gotOk = %v, want %v", gotOk, tt.wantOk)
				return
			}
			if gotOk {
				if got.Name != tt.want.Name {
					t.Errorf("deckhouseReleaseProcessing() Name = %v, want %v", got.Name, tt.want.Name)
				}
				if got.Phase != tt.want.Phase {
					t.Errorf("deckhouseReleaseProcessing() Phase = %v, want %v", got.Phase, tt.want.Phase)
				}
				if got.TransitionTime != tt.want.TransitionTime {
					t.Errorf("deckhouseReleaseProcessing() TransitionTime = %v, want %v", got.TransitionTime, tt.want.TransitionTime)
				}
				if got.Message != tt.want.Message {
					t.Errorf("deckhouseReleaseProcessing() Message = %v, want %v", got.Message, tt.want.Message)
				}
			}
		})
	}
}
