/*
Copyright 2024 Flant JSC

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
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

func TestNewFilter(t *testing.T) {
	tests := []struct {
		name           string
		expressions    []string
		expectedModule string
		wantErr        bool
	}{
		{
			name:        "empty expressions",
			expressions: []string{},
			wantErr:     false,
		},
		{
			name:        "module without version",
			expressions: []string{"module"},
			wantErr:     false,
		},
		{
			name:        "valid expression",
			expressions: []string{"module@1.2.3"},
			wantErr:     false,
		},
		{
			name:        "multiple valid expressions",
			expressions: []string{"module1@1.2.3", "module2@2.3.4"},
			wantErr:     false,
		},
		{
			name:        "empty module name",
			expressions: []string{" @1.2.3"},
			wantErr:     true,
		},
		{
			name:        "duplicate module is merged, not rejected",
			expressions: []string{"module@1.2.3", "module@2.3.4"},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewFilter(tt.expressions, FilterTypeWhitelist)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			filter.UseLogger(log.NewSLogger(slog.LevelDebug))

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, filter)
				require.NotNil(t, filter.modules)
			}
		})
	}
}

// TestNewFilter_VersionParsing tests version parsing specifically
func TestNewFilter_VersionParsing(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		wantErr    bool
	}{
		{
			name:       "valid version",
			expression: "module@1.2.3",
			wantErr:    false,
		},
		{
			name:       "invalid version",
			expression: "module@invalid",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewFilter([]string{tt.expression}, FilterTypeWhitelist)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			filter.UseLogger(log.NewSLogger(slog.LevelDebug))

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, filter)
				v, ok := filter.modules[strings.Split(tt.expression, "@")[0]]
				require.True(t, ok)
				require.NotNil(t, v)
			}
		})
	}
}

// TestFilter_BareVersionConstraint exercises the full --include-module parse
// path (NewFilter -> parseVersionConstraint -> NewImplicitVersionConstraint)
// for operator-less version literals. The 0.x case is the regression guard:
// a bare `0.4.0` must span the whole 0.x line, not lock to the 0.4 minor the
// way a caret (`^0.4.0` == `>=0.4.0 <0.5.0`) would.
func TestFilter_BareVersionConstraint(t *testing.T) {
	logger := log.NewSLogger(slog.LevelDebug)

	tests := []struct {
		name       string
		expression string
		releases   []string
		want       []string
	}{
		{
			// Regression: bare 0.x version must reach every intermediate minor
			// up to (but not including) the next major, keeping only the latest
			// patch per minor. Caret would have stopped at <0.5.0.
			name:       "bare 0.x version spans the whole 0.x line",
			expression: "module1@0.4.0",
			releases: []string{
				"v0.4.2", "v0.4.4",
				"v0.5.1", "v0.5.3",
				"v0.6.1",
				"v0.7.2",
			},
			want: []string{"v0.4.4", "v0.5.3", "v0.6.1", "v0.7.2"},
		},
		{
			// A bare >=1 version is unchanged from the old caret behaviour:
			// spans the current major, latest patch per minor, no 2.x.
			name:       "bare 1.x version keeps caret-equivalent behaviour",
			expression: "module1@1.52.0",
			releases: []string{
				"v1.52.0",
				"v1.53.1", "v1.53.2",
				"v1.54.1",
				"v1.55.1",
				"v2.0.0",
			},
			want: []string{"v1.52.0", "v1.53.2", "v1.54.1", "v1.55.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewFilter([]string{tt.expression}, FilterTypeWhitelist)
			require.NoError(t, err)
			filter.UseLogger(logger)

			// A bare version is non-exact, so release channels must be mirrored.
			require.True(t, filter.ShouldMirrorReleaseChannels("module1"))

			mod := &Module{Name: "module1", Releases: tt.releases}
			require.ElementsMatch(t, tt.want, filter.VersionsToMirror(mod))
		})
	}
}

func TestFilter_Match(t *testing.T) {
	logger := log.NewSLogger(slog.LevelDebug)
	type args struct {
		mod *Module
	}
	tests := []struct {
		name string
		f    Filter
		args args
		want bool
	}{
		{
			name: "[whitelist] empty filter",
			f: Filter{
				_type:   FilterTypeWhitelist,
				modules: map[string]VersionConstraint{},
				logger:  logger,
			},
			args: args{
				mod: &Module{Name: "module1"},
			},
			want: false,
		},
		{
			name: "[whitelist] match",
			f: Filter{
				_type: FilterTypeWhitelist,
				modules: map[string]VersionConstraint{
					"module1": NewExactTagConstraint("v12.34.56"),
					"module2": NewExactTagConstraint("v0.0.1"),
				},
				logger: logger,
			},
			args: args{
				mod: &Module{Name: "module1"},
			},
			want: true,
		},
		{
			name: "[whitelist] no match",
			f: Filter{
				_type: FilterTypeWhitelist,
				modules: map[string]VersionConstraint{
					"module1": NewExactTagConstraint("v12.34.56"),
					"module2": NewExactTagConstraint("v0.0.1"),
				},
				logger: logger,
			},
			args: args{
				mod: &Module{Name: "module3"},
			},
			want: false,
		},
		{
			name: "[blacklist] empty filter",
			f: Filter{
				_type:   FilterTypeBlacklist,
				modules: map[string]VersionConstraint{},
				logger:  logger,
			},
			args: args{
				mod: &Module{Name: "module1"},
			},
			want: true,
		},
		{
			name: "[blacklist] match",
			f: Filter{
				_type: FilterTypeBlacklist,
				modules: map[string]VersionConstraint{
					"module1": NewExactTagConstraint("v12.34.56"),
					"module2": NewExactTagConstraint("v0.0.1"),
				},
				logger: logger,
			},
			args: args{
				mod: &Module{Name: "module1"},
			},
			want: false,
		},
		{
			name: "[blacklist] no match",
			f: Filter{
				_type: FilterTypeBlacklist,
				modules: map[string]VersionConstraint{
					"module1": NewExactTagConstraint("v12.34.56"),
					"module2": NewExactTagConstraint("v0.0.1"),
				},
				logger: logger,
			},
			args: args{
				mod: &Module{Name: "module3"},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.f.Match(tt.args.mod), "Match(%v)", tt.args.mod)
		})
	}
}

func TestFilter_VersionsToMirror(t *testing.T) {
	logger := log.NewSLogger(slog.LevelDebug)

	geConstraint := func(v string) VersionConstraint {
		c, err := NewSemanticVersionConstraint(v)
		require.NoError(t, err)
		return c
	}

	tests := []struct {
		name   string
		filter Filter
		mod    *Module
		want   []string
	}{
		{
			// Caret constraint: keep highest patch in every (major, minor)
			// bucket inside ^1.3.0 = >=1.3.0 <2.0.0. The bucket 1.3.x
			// degenerates to the single tag v1.3.0 (so v1.3.0 stays); 1.4.x
			// has only v1.4.1 (so v1.4.1 stays). v1.0.0..v1.2.0 are below the
			// constraint and stay out.
			name: "happy path: semver constraint ^ keeps only latest patch per minor",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint("^1.3.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					internal.AlphaChannel,
					internal.BetaChannel,
					internal.EarlyAccessChannel,
					internal.StableChannel,
					internal.RockSolidChannel,
					"v1.0.0", "v1.1.0", "v1.2.0", "v1.3.0", "v1.4.1"},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.3.0", "v1.4.1"},
		},
		{
			// Caret constraint with multiple patches in the same minor: only
			// the highest patch (v1.3.3) survives the per-minor filter.
			// v1.3.0 is dropped on purpose. This is the regression case for
			// issue #220 (`code@v1.6.0` pulling every 1.6.x patch). Channel
			// aliases are appended by the test harness because the constraint
			// is non-exact (ShouldMirrorReleaseChannels=true).
			name: "semver constraint ^ drops older patches in same minor",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint("^1.3.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					"v1.0.0", "v1.1.0", "v1.2.0",
					"v1.3.0", "v1.3.1", "v1.3.2", "v1.3.3",
					"v1.4.0", "v1.4.1"},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.3.3", "v1.4.1"},
		},
		{
			// Tilde constraint = >=1.3.0 <1.4.0, which is one (major, minor)
			// bucket. Latest patch v1.3.3 is the only output.
			name: "semver constraint tilde ~ (>=1.3.0 <1.4.0) keeps only the latest patch",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint("~1.3.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					internal.AlphaChannel,
					internal.BetaChannel,
					internal.EarlyAccessChannel,
					internal.StableChannel,
					internal.RockSolidChannel,
					"v1.0.0", "v1.1.0", "v1.2.0", "v1.3.0", "v1.3.3", "v1.4.1"},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.3.3"},
		},
		{
			// Explicit range: each minor bucket inside [1.1.0, 1.3.0) collapses
			// to its highest patch — here the registry has only one tag per
			// minor, so all matched versions survive.
			name: "semver constraint range >=1.1.0 <1.3.0",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint(">=1.1.0 <1.3.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					internal.AlphaChannel,
					internal.BetaChannel,
					internal.EarlyAccessChannel,
					internal.StableChannel,
					internal.RockSolidChannel,
					"v1.0.0", "v1.1.0", "v1.2.0", "v1.3.0", "v1.4.1"},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.1.0", "v1.2.0"},
		},
		{
			// Explicit range with multiple patches per minor: collapse to the
			// highest patch in each (major, minor) AND keep the >= anchor.
			// `>=1.6.0` literally names v1.6.0 — the equality is part of the
			// operator, so v1.6.0 must round-trip. v1.7.x has no anchor (the
			// upper bound `<1.8.0` is exclusive) so 1.7.x degenerates to its
			// latest patch v1.7.1.
			name: "semver range collapses non-anchor minors but preserves >= anchor",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint(">=1.6.0 <1.8.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					"v1.6.0", "v1.6.1", "v1.6.2", "v1.6.3", "v1.6.4", "v1.6.5",
					"v1.7.0", "v1.7.1",
					"v1.8.0",
				},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.6.0", "v1.6.5", "v1.7.1"},
		},
		{
			// Bare `>=` constraint with no upper bound. The anchor is v1.40.0
			// and it sits in a minor that has a newer patch (v1.40.1). Under
			// pure latest-patch-per-minor semantics v1.40.0 would be dropped;
			// the anchor exception keeps it in the result. v1.41.x has no
			// anchor and collapses to its latest patch.
			name: "bare >= preserves anchor and keeps latest patch in same minor",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint(">=1.40.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					"v1.39.5",
					"v1.40.0", "v1.40.1",
					"v1.41.0", "v1.41.2",
				},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.40.0", "v1.40.1", "v1.41.2"},
		},
		{
			// `<=` is also an inclusive boundary. v1.42.5 must round-trip
			// even though latest-patch-per-minor would prefer v1.42.7.
			// Anchors stack: the lower bound `>=1.40.0` and the upper bound
			// `<=1.42.5` are both honoured.
			name: "<= preserves upper-bound anchor",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint(">=1.40.0 <=1.42.5"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					"v1.40.0", "v1.40.1",
					"v1.41.0",
					"v1.42.0", "v1.42.5",
					"v1.42.7", // Filtered out: above the upper bound.
				},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.5"},
		},
		{
			// Strict `>` is exclusive — the named version is NOT an anchor
			// because the user explicitly excluded it. v1.40.0 must NOT
			// appear in the result; the 1.40.x bucket has only v1.40.1
			// (which sits inside the >, so it stays via latest-patch).
			name: "strict > does not create anchor",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint(">1.40.0 <1.42.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					"v1.40.0", "v1.40.1",
					"v1.41.0", "v1.41.3",
				},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.40.1", "v1.41.3"},
		},
		{
			// Caret is shorthand for a range — its lower bound is NOT an
			// anchor. `^1.6.0` expands to `>=1.6.0 <2.0.0`; the implicit `>=`
			// must not preserve v1.6.0 (issue #220 case: the user wrote
			// `module@v1.6.0` and wants only the latest patch per minor).
			name: "caret does not create anchor (issue #220 base case)",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint("^1.6.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					"v1.6.0", "v1.6.1", "v1.6.2", "v1.6.3", "v1.6.4", "v1.6.5",
					"v1.7.0", "v1.7.1",
				},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.6.5", "v1.7.1"},
		},
		{
			// Anchor refers to a tag the registry doesn't carry. We must
			// not invent a tag — restoreInclusiveAnchors only restores
			// anchors that exist in `available`. Result is just the
			// latest-patch-per-minor set.
			name: ">= anchor that the registry does not have is silently skipped",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": geConstraint(">=1.40.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					"v1.40.1", "v1.40.2",
					"v1.41.0",
				},
			},
			want: []string{
				internal.AlphaChannel,
				internal.BetaChannel,
				internal.EarlyAccessChannel,
				internal.StableChannel,
				internal.RockSolidChannel,
				"v1.40.2", "v1.41.0"},
		},
		{
			name: "happy path: exact match",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": NewExactTagConstraint("v1.3.0"),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					internal.AlphaChannel,
					internal.BetaChannel,
					internal.EarlyAccessChannel,
					internal.StableChannel,
					internal.RockSolidChannel,
					"v1.0.0", "v1.1.0", "v1.2.0", "v1.3.0", "v1.3.3", "v1.4.1"},
			},
			want: []string{"v1.3.0"},
		},
		{
			// Several exact tags pinned for the same name (the user repeated
			// --include-package test@=vX). They merge into a MultiConstraint
			// of exacts, which stays exact so no release channels are added,
			// and every pinned tag is pulled.
			name: "multiple exact tags pinned for the same name pull all of them",
			filter: Filter{
				logger: logger,
				modules: map[string]VersionConstraint{
					"module1": mergeConstraints(
						mergeConstraints(
							NewExactTagConstraint("v0.0.2"),
							NewExactTagConstraint("v0.0.3"),
						),
						NewExactTagConstraint("v0.0.8"),
					),
				},
			},
			mod: &Module{
				Name: "module1",
				Releases: []string{
					internal.AlphaChannel,
					internal.StableChannel,
					"v0.0.1", "v0.0.2", "v0.0.3", "v0.0.4", "v0.0.8"},
			},
			want: []string{"v0.0.2", "v0.0.3", "v0.0.8"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			if tt.filter.ShouldMirrorReleaseChannels(tt.mod.Name) {
				got = append(got, internal.GetAllDefaultReleaseChannels()...)
			}
			got = append(got, tt.filter.VersionsToMirror(tt.mod)...)
			require.ElementsMatch(t, tt.want, got)
			require.Len(t, got, len(tt.want))
		})
	}
}
