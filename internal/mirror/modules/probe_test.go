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

package modules

import (
	"context"
	"errors"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSemanticVersionConstraint_LowerBound(t *testing.T) {
	tests := []struct {
		name           string
		constraint     string
		wantLowerBound string
	}{
		{
			name:           "implicit caret",
			constraint:     "^1.65.0",
			wantLowerBound: "1.65.0",
		},
		{
			name:           "tilde",
			constraint:     "~1.65.0",
			wantLowerBound: "1.65.0",
		},
		{
			name:           "range with two inclusive anchors",
			constraint:     ">=1.64.0 <=1.68.0",
			wantLowerBound: "1.64.0",
		},
		{
			name:           "range with greater-than (no equals) lower bound",
			constraint:     ">1.0.0 <=1.5.0",
			wantLowerBound: "1.0.0",
		},
		{
			name:           "lower bound smallest even when written in reversed order",
			constraint:     "<=1.68.0 >=1.64.0",
			wantLowerBound: "1.64.0",
		},
		{
			name:           "constraint with v-prefixed version literal",
			constraint:     ">=v1.50.0 <2.0.0",
			wantLowerBound: "1.50.0",
		},
		{
			name:           "major-only literal expands to .0.0",
			constraint:     ">=1.64 <=1.68",
			wantLowerBound: "1.64.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewSemanticVersionConstraint(tt.constraint)
			require.NoError(t, err)
			require.NotNil(t, c.LowerBound())
			assert.Equal(t, tt.wantLowerBound, c.LowerBound().String())
		})
	}
}

// fakeRegistry exposes a deterministic version set to the probe so test
// expectations can match exact tag traversals.
type fakeRegistry struct {
	have  map[string]struct{}
	calls []string
	err   error
}

func newFakeRegistry(versions ...string) *fakeRegistry {
	have := make(map[string]struct{}, len(versions))
	for _, v := range versions {
		have[v] = struct{}{}
	}
	return &fakeRegistry{have: have}
}

func (r *fakeRegistry) check(_ context.Context, v *semver.Version) (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	r.calls = append(r.calls, v.String())
	_, ok := r.have[v.String()]
	return ok, nil
}

func TestProbeAvailableVersions(t *testing.T) {
	// wantCalls reflects ONLY versions that pass constraint.Match — the
	// probe short-circuits out-of-constraint candidates before touching
	// the registry, which is the whole point of having a bounded probe.
	tests := []struct {
		name         string
		constraint   string
		registryHas  []string
		wantVersions []string
		wantCalls    []string
	}{
		{
			name:         "walk full patch series then stop on missing minor and out-of-range major",
			constraint:   "^1.64.0",
			registryHas:  []string{"1.64.0", "1.64.1", "1.64.2"},
			wantVersions: []string{"1.64.0", "1.64.1", "1.64.2"},
			wantCalls: []string{
				"1.64.0", "1.64.1", "1.64.2", "1.64.3", // patch ends on missing
				"1.65.0", // next minor — missing
				// next major 2.0.0 is outside ^1.64.0; no registry call made
			},
		},
		{
			name:         "skip a missing starting patch but pick up the next minor",
			constraint:   ">=1.64.0 <2.0.0",
			registryHas:  []string{"1.65.0", "1.65.1"},
			wantVersions: []string{"1.65.0", "1.65.1"},
			wantCalls: []string{
				"1.64.0", // missing — falls into "try next minor"
				"1.65.0", // next minor — found, resume patch
				"1.65.1", // patch continues
				"1.65.2", // patch ends on missing
				"1.66.0", // next minor — missing
				// 2.0.0 outside constraint; not probed
			},
		},
		{
			name:         "respects upper bound from inclusive range",
			constraint:   ">=1.64.0 <=1.65.0",
			registryHas:  []string{"1.64.0", "1.64.1", "1.65.0", "1.65.1", "1.65.2"},
			wantVersions: []string{"1.64.0", "1.64.1", "1.65.0"},
			wantCalls: []string{
				"1.64.0", "1.64.1", "1.64.2", // patch ends on missing
				"1.65.0", // next minor — found, resume patch
				// 1.65.1 fails constraint (<=1.65.0); no registry call.
				// next minor / major also outside constraint; no calls.
			},
		},
		{
			name:         "advances across major boundary when minor lookahead fails",
			constraint:   ">=1.0.0 <3.0.0",
			registryHas:  []string{"1.0.0", "2.0.0", "2.0.1"},
			wantVersions: []string{"1.0.0", "2.0.0", "2.0.1"},
			wantCalls: []string{
				"1.0.0", "1.0.1", // patch ends
				"1.1.0",          // next minor — missing
				"2.0.0",          // next major — found, resume patch
				"2.0.1", "2.0.2", // patch ends
				"2.1.0", // next minor — missing
				// 3.0.0 outside constraint; no probe call
			},
		},
		{
			name:         "empty when nothing exists in either jump",
			constraint:   "^1.99.0",
			registryHas:  nil,
			wantVersions: []string{},
			wantCalls: []string{
				"1.99.0",  // patch ends on missing
				"1.100.0", // next minor — missing (still in ^1.99.0 = <2.0.0)
				// 2.0.0 outside ^1.99.0; no probe call
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewSemanticVersionConstraint(tt.constraint)
			require.NoError(t, err)

			reg := newFakeRegistry(tt.registryHas...)

			got, err := ProbeAvailableVersions(context.Background(), c, reg.check)
			require.NoError(t, err)

			gotStrs := make([]string, 0, len(got))
			for _, v := range got {
				gotStrs = append(gotStrs, v.String())
			}
			assert.Equal(t, tt.wantVersions, gotStrs, "discovered versions mismatch")
			assert.Equal(t, tt.wantCalls, reg.calls, "probe call sequence mismatch")
		})
	}
}

func TestProbeAvailableVersions_PropagatesRegistryError(t *testing.T) {
	c, err := NewSemanticVersionConstraint("^1.0.0")
	require.NoError(t, err)

	sentinel := errors.New("registry down")
	reg := &fakeRegistry{err: sentinel}

	_, err = ProbeAvailableVersions(context.Background(), c, reg.check)
	require.ErrorIs(t, err, sentinel)
}

func TestProbeAvailableVersions_RespectsContextCancellation(t *testing.T) {
	c, err := NewSemanticVersionConstraint("^1.0.0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = ProbeAvailableVersions(ctx, c, func(_ context.Context, _ *semver.Version) (bool, error) {
		// Should never reach the registry — context is already cancelled.
		t.Fatal("checker called after context cancellation")
		return false, nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestProbeAvailableVersions_RejectsNilArguments(t *testing.T) {
	_, err := ProbeAvailableVersions(context.Background(), nil, func(_ context.Context, _ *semver.Version) (bool, error) {
		return false, nil
	})
	require.Error(t, err)

	c, err := NewSemanticVersionConstraint("^1.0.0")
	require.NoError(t, err)
	_, err = ProbeAvailableVersions(context.Background(), c, nil)
	require.Error(t, err)
}
