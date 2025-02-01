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

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			name:        "duplicate module",
			expressions: []string{"module@1.2.3", "module@2.3.4"},
			wantErr:     true,
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
				modules: map[string]*semver.Version{},
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
				modules: map[string]*semver.Version{
					"module1": semver.MustParse("v12.34.56"),
					"module2": semver.MustParse("v0.0.1"),
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
				modules: map[string]*semver.Version{
					"module1": semver.MustParse("v12.34.56"),
					"module2": semver.MustParse("v0.0.1"),
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
				modules: map[string]*semver.Version{},
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
				modules: map[string]*semver.Version{
					"module1": semver.MustParse("v12.34.56"),
					"module2": semver.MustParse("v0.0.1"),
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
				modules: map[string]*semver.Version{
					"module1": semver.MustParse("v12.34.56"),
					"module2": semver.MustParse("v0.0.1"),
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

func TestFilter_FilterReleases(t *testing.T) {
	logger := log.NewSLogger(slog.LevelDebug)
	tests := []struct {
		name   string
		filter Filter
		mod    *Module
		want   []string
	}{
		{
			name: "happy path",
			filter: Filter{
				logger: logger,
				modules: map[string]*semver.Version{
					"module1": semver.MustParse("v1.3.0"),
					"module2": semver.MustParse("2.1.47"),
				},
			},
			mod: &Module{
				Name:     "module1",
				Releases: []string{"alpha", "beta", "early-access", "stable", "rock-solid", "v1.0.0", "v1.1.0", "v1.2.0", "v1.3.0", "v1.4.1"},
			},
			want: []string{"alpha", "beta", "early-access", "stable", "rock-solid", "v1.3.0", "v1.4.1"},
		},
		{
			name: "module not in filter",
			filter: Filter{
				logger: logger,
				modules: map[string]*semver.Version{
					"module1": semver.MustParse("v1.3.0"),
					"module2": semver.MustParse("2.1.47")},
			},
			mod: &Module{
				Name:     "module",
				Releases: []string{"alpha", "beta", "early-access", "stable", "rock-solid", "v1.0.0", "v1.1.0", "v1.2.0", "v1.3.0", "v1.4.1"},
			},
			want: []string{"alpha", "beta", "early-access", "stable", "rock-solid", "v1.0.0", "v1.1.0", "v1.2.0", "v1.3.0", "v1.4.1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.filter.FilterReleases(tt.mod)
			require.ElementsMatch(t, tt.want, tt.mod.Releases)
			require.Len(t, tt.mod.Releases, len(tt.want))
		})
	}
}
