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

package naming_test

import (
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/naming"
)

func TestShadowName(t *testing.T) {
	t.Parallel()

	// Known artifact name -> expected shadow name (precomputed from the
	// sha256-based formula: "d8-ss-" + hex(sha256(input)[:8])).
	cases := []struct {
		artifactName string
		want         string
	}{
		{
			artifactName: "snapcontent-00000000-0000-0000-0000-000000000000",
			want:         "d8-ss-b00bb79634e3d64f",
		},
		{
			artifactName: "short",
			want:         "d8-ss-f9b0078b5df596d2",
		},
		{
			artifactName: "foo",
			want:         "d8-ss-2c26b46b68ffc68f",
		},
		{
			artifactName: "bar",
			want:         "d8-ss-fcde2b2edba56bf4",
		},
		{
			artifactName: "snapcontent-aaaabbbb-cccc-dddd-eeee-ffffffffffff",
			want:         "d8-ss-43f8e9248e90956f",
		},
	}

	for _, tc := range cases {
		t.Run(tc.artifactName, func(t *testing.T) {
			t.Parallel()

			got := naming.ShadowName(tc.artifactName)

			if got != tc.want {
				t.Errorf("ShadowName(%q) = %q, want %q", tc.artifactName, got, tc.want)
			}

			if len(got) != 22 {
				t.Errorf("ShadowName(%q) length = %d, want 22", tc.artifactName, len(got))
			}

			if len(got) > 63 {
				t.Errorf("ShadowName(%q) length = %d, exceeds 63-char DNS label limit", tc.artifactName, len(got))
			}

			if !strings.HasPrefix(got, "d8-ss-") {
				t.Errorf("ShadowName(%q) = %q, must start with \"d8-ss-\"", tc.artifactName, got)
			}
		})
	}
}

func TestShadowName_Determinism(t *testing.T) {
	t.Parallel()

	const input = "snapcontent-12345678-1234-1234-1234-123456789abc"

	a := naming.ShadowName(input)
	b := naming.ShadowName(input)

	if a != b {
		t.Errorf("ShadowName is not deterministic: %q != %q", a, b)
	}
}

func TestShadowName_CollisionResistance(t *testing.T) {
	t.Parallel()

	// Distinct inputs must produce distinct names.
	pairs := [][2]string{
		{"foo", "bar"},
		{"snapcontent-aaa", "snapcontent-bbb"},
		{"", "x"},
	}

	for _, p := range pairs {
		a := naming.ShadowName(p[0])
		b := naming.ShadowName(p[1])

		if a == b {
			t.Errorf("ShadowName(%q) == ShadowName(%q) == %q (collision)", p[0], p[1], a)
		}
	}
}

func TestShadowName_LongInput(t *testing.T) {
	t.Parallel()

	// A very long artifact name must still produce a <=63-char result.
	long := "snapcontent-" + strings.Repeat("a", 300)
	got := naming.ShadowName(long)

	if len(got) > 63 {
		t.Errorf("ShadowName(long) length = %d, exceeds 63-char DNS label limit", len(got))
	}

	if len(got) != 22 {
		t.Errorf("ShadowName(long) length = %d, want 22", len(got))
	}
}
