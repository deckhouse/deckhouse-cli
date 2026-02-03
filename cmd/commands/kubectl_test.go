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

package commands

import (
	"testing"
)

func TestD8CommandRegex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "backtick with replace command",
			input:    "You can run `d8 replace -f /tmp/d8-edit-1481875006.yaml` to try this update again.",
			expected: "You can run `d8 k replace -f /tmp/d8-edit-1481875006.yaml` to try this update again.",
		},
		{
			name:     "backtick with edit command",
			input:    "You can run `d8 edit deployment/my-app` to edit the resource.",
			expected: "You can run `d8 k edit deployment/my-app` to edit the resource.",
		},
		{
			name:     "double quotes with apply command",
			input:    `Run "d8 apply -f file.yaml" to apply changes.`,
			expected: `Run "d8 k apply -f file.yaml" to apply changes.`,
		},
		{
			name:     "single quotes with get command",
			input:    "Try 'd8 get pods' to see the pods.",
			expected: "Try 'd8 k get pods' to see the pods.",
		},
		{
			name:     "multiple commands in one line",
			input:    "Use `d8 get pods` or `d8 describe pod` for details.",
			expected: "Use `d8 k get pods` or `d8 k describe pod` for details.",
		},
		{
			name:     "backtick with delete command",
			input:    "Execute `d8 delete pod/nginx` to remove the pod.",
			expected: "Execute `d8 k delete pod/nginx` to remove the pod.",
		},
		{
			name: "webhook error message",
			input: `error: moduleconfigs.deckhouse.io "global" could not be patched: Internal error occurred: failed calling webhook "module-configs.deckhouse-webhook.deckhouse.io": failed to call webhook: Post "https://deckhouse.d8-system.svc:4223/validate/v1alpha1/module-configs?timeout=10s": dial tcp 10.222.64.246:4223: connect: operation not permitted
You can run ` + "`d8 replace -f /tmp/d8-edit-1481875006.yaml`" + ` to try this update again.`,
			expected: `error: moduleconfigs.deckhouse.io "global" could not be patched: Internal error occurred: failed calling webhook "module-configs.deckhouse-webhook.deckhouse.io": failed to call webhook: Post "https://deckhouse.d8-system.svc:4223/validate/v1alpha1/module-configs?timeout=10s": dial tcp 10.222.64.246:4223: connect: operation not permitted
You can run ` + "`d8 k replace -f /tmp/d8-edit-1481875006.yaml`" + ` to try this update again.`,
		},
		{
			name:     "no match - d8 without quote",
			input:    "Run d8 apply without quotes",
			expected: "Run d8 apply without quotes",
		},
		{
			name:     "no match - quoted text without d8",
			input:    "Use `kubectl get pods` instead.",
			expected: "Use `kubectl get pods` instead.",
		},
		{
			name:     "command with hyphenated subcommand",
			input:    "Try `d8 top-nodes` to see node metrics.",
			expected: "Try `d8 k top-nodes` to see node metrics.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := d8CommandRegex.ReplaceAllString(tt.input, "${1}d8 k ${2}")
			if result != tt.expected {
				t.Errorf("\nInput:    %s\nExpected: %s\nGot:      %s", tt.input, tt.expected, result)
			}
		})
	}
}

func TestD8CommandRegexMatches(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		shouldMatch   bool
		expectedMatch string
	}{
		{
			name:          "matches backtick",
			input:         "`d8 replace -f file.yaml`",
			shouldMatch:   true,
			expectedMatch: "`d8 replace",
		},
		{
			name:          "matches double quote",
			input:         `"d8 apply -f file.yaml"`,
			shouldMatch:   true,
			expectedMatch: `"d8 apply`,
		},
		{
			name:          "matches single quote",
			input:         "'d8 get pods'",
			shouldMatch:   true,
			expectedMatch: "'d8 get",
		},
		{
			name:        "no match without quote",
			input:       "d8 apply -f file.yaml",
			shouldMatch: false,
		},
		{
			name:        "no match with space before quote",
			input:       "Run d8 `apply -f file.yaml`",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match := d8CommandRegex.FindString(tt.input)
			if tt.shouldMatch {
				if match == "" {
					t.Errorf("Expected to match %q, but got no match", tt.expectedMatch)
				} else if match != tt.expectedMatch {
					t.Errorf("Expected match %q, but got %q", tt.expectedMatch, match)
				}
			} else {
				if match != "" {
					t.Errorf("Expected no match, but got %q", match)
				}
			}
		})
	}
}

func TestD8CommandRegexCaptureGroups(t *testing.T) {
	input := "`d8 replace -f file.yaml`"
	matches := d8CommandRegex.FindStringSubmatch(input)

	if len(matches) != 3 {
		t.Fatalf("Expected 3 capture groups (full match + 2 groups), got %d", len(matches))
	}

	expectedFullMatch := "`d8 replace"
	expectedGroup1 := "`"
	expectedGroup2 := "replace"

	if matches[0] != expectedFullMatch {
		t.Errorf("Full match: expected %q, got %q", expectedFullMatch, matches[0])
	}
	if matches[1] != expectedGroup1 {
		t.Errorf("Group 1 (quote): expected %q, got %q", expectedGroup1, matches[1])
	}
	if matches[2] != expectedGroup2 {
		t.Errorf("Group 2 (command): expected %q, got %q", expectedGroup2, matches[2])
	}
}
