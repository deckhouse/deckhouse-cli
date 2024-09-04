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
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

func TestParseFilterString(t *testing.T) {
	logger := log.NewSLogger(slog.LevelDebug)
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		want    Filter
		wantErr string
	}{
		{
			name: "Empty filter expression",
			args: args{str: ""},
			want: Filter{
				modules: map[string]*semver.Version{},
				logger:  logger,
			},
		},
		{
			name: "One filter expression",
			args: args{str: "moduleName@v12.34.56"},
			want: Filter{
				modules: map[string]*semver.Version{"moduleName": semver.MustParse("v12.34.56")},
				logger:  logger,
			},
		},
		{
			name:    "Multiple filter expression for one module",
			args:    args{str: "moduleName@v12.34.56;moduleName@v0.0.1;"},
			want:    Filter{modules: map[string]*semver.Version{}, logger: logger},
			wantErr: "declared multiple times",
		},
		{
			name: "Multiple filter expression for different modules",
			args: args{str: "module1@v12.34.56;module2@v0.0.1;"},
			want: Filter{
				modules: map[string]*semver.Version{"module1": semver.MustParse("v12.34.56"), "module2": semver.MustParse("v0.0.1")},
				logger:  logger,
			},
		},
		{
			name: "Multiple filter expression for different modules with bad spacing and sloppy formatting",
			args: args{str: " ; module1 @1.1.1;module2 @ v2.3.2; "},
			want: Filter{
				modules: map[string]*semver.Version{"module1": semver.MustParse("v1.1.1"), "module2": semver.MustParse("v2.3.2")},
				logger:  logger,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewFilter(tt.args.str, logger)
			if tt.wantErr != "" && assert.ErrorContains(t, err, tt.wantErr) {
				return
			}

			require.Len(t, got.modules, len(tt.want.modules))

			for moduleName, minVersion := range tt.want.modules {
				require.Contains(t, got.modules, moduleName)
				require.Condition(t, func() bool {
					return minVersion.Equal(got.modules[moduleName])
				})
			}
		})
	}
}

func TestFilter_MatchesFilter(t *testing.T) {
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
			name: "empty filter",
			f: Filter{
				modules: map[string]*semver.Version{},
				logger:  logger,
			},
			args: args{
				mod: &Module{Name: "module1"},
			},
			want: false,
		},
		{
			name: "match",
			f: Filter{
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
			name: "no match",
			f: Filter{
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.f.MatchesFilter(tt.args.mod), "MatchesFilter(%v)", tt.args.mod)
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
