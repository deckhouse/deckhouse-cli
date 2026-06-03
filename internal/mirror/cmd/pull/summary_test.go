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

package pull

import (
	"strings"
	"testing"
	"time"

	"github.com/fatih/color"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
)

func TestRenderPullSummary(t *testing.T) {
	// Make the bold header deterministic across TTY/non-TTY test runs.
	color.NoColor = true

	manyModules := make([]mirror.ModuleStat, 0, 20)
	for i := range 20 {
		manyModules = append(manyModules, mirror.ModuleStat{Name: "mod" + string(rune('a'+i)), Images: i + 1})
	}

	tests := []struct {
		name         string
		summary      *mirror.PullSummary
		verbose      bool
		contains     []string
		notContains  []string
		skippedCount int // -1 to skip the assertion
	}{
		{
			name: "real pull verbose lists modules",
			summary: &mirror.PullSummary{
				Elapsed:   14*time.Minute + 32*time.Second,
				Platform:  mirror.ComponentStats{Attempted: true, Images: 412},
				Installer: mirror.ComponentStats{Attempted: true, Images: 3},
				Security:  mirror.SecurityStats{Attempted: true, Available: true, Databases: 4, AvailableDatabases: 4},
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 100,
					Modules: []mirror.ModuleStat{
						{Name: "commander", Images: 57},
						{Name: "console", Images: 43},
					},
				},
				Bundle: mirror.BundleStats{
					TotalBytes: 9_800_000_000,
					Files: []mirror.BundleFile{
						{Name: "module-commander.tar", Bytes: 801_500_000, Chunks: 2},
						{Name: "platform.tar", Bytes: 3_100_000_000},
					},
				},
			},
			verbose: true,
			contains: []string{
				"Pull summary",
				"Platform:", "included",
				"Installer:",
				"Security:", "4/4 databases",
				"2 modules",
				"commander", "console",
				"Bundle artifacts (3 files)", // 2 chunks + 1 single tar
				"(2 chunks)",
				"TOTAL",
				"Elapsed: 14m32s",
			},
			notContains:  []string{"dry-run", "skipped", "cancelled", "images"},
			skippedCount: 0,
		},
		{
			name: "platform releases shown on the platform line",
			summary: &mirror.PullSummary{
				Platform: mirror.ComponentStats{
					Attempted: true, Images: 327,
					Versions: []string{"v1.69.0"},
					Channels: []string{"alpha", "beta", "early-access", "stable", "rock-solid"},
				},
			},
			verbose: false,
			contains: []string{
				"v1.69.0 (5 channels)",
			},
			// No Edition set -> no Edition line.
			notContains:  []string{"327", "Edition"},
			skippedCount: -1,
		},
		{
			name: "edition shown above platform when known",
			summary: &mirror.PullSummary{
				Edition:  "ce",
				Platform: mirror.ComponentStats{Attempted: true, Versions: []string{"v1.75.9"}, Channels: []string{"a", "b", "c", "d", "e"}},
			},
			verbose: false,
			contains: []string{
				"Edition:", "CE", // uppercased for display
				"v1.75.9 (5 channels)",
			},
			skippedCount: -1,
		},
		{
			name: "module versions listed per module in verbose",
			summary: &mirror.PullSummary{
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 84,
					TotalVEX:    17,
					Modules: []mirror.ModuleStat{
						{Name: "code", Images: 84, VEX: 17, Versions: []string{"v1.10.3", "v1.9.16"}},
					},
				},
			},
			verbose: true,
			contains: []string{
				"code", "[v1.10.3, v1.9.16]", "(17 VEX)", "17 VEXes",
			},
			notContains:  []string{"84", "images", "including"},
			skippedCount: -1,
		},
		{
			name: "module versions are sorted newest-first by semver",
			summary: &mirror.PullSummary{
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 14,
					Modules: []mirror.ModuleStat{
						// Resolved in arbitrary order; must render sorted, newest first.
						{Name: "code", Images: 14, Versions: []string{"v1.5.4", "v10.0.1", "v1.10.3", "v1.9.16"}},
					},
				},
			},
			verbose:      true,
			contains:     []string{"[v10.0.1, v1.10.3, v1.9.16, v1.5.4]"},
			skippedCount: -1,
		},
		{
			name: "default hides per-module breakdown",
			summary: &mirror.PullSummary{
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 100,
					Modules: []mirror.ModuleStat{
						{Name: "commander", Images: 57},
						{Name: "console", Images: 43},
					},
				},
			},
			verbose: false,
			contains: []string{
				"2 modules",
			},
			// Without --verbose-summary the individual module names must not appear.
			notContains:  []string{"commander", "console", "images"},
			skippedCount: -1,
		},
		{
			name: "verbose lists every module without truncation",
			summary: &mirror.PullSummary{
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 210,
					Modules:     manyModules,
				},
			},
			verbose: true,
			contains: []string{
				"20 modules",
				"moda", "modt", // first and last of the 20
			},
			notContains:  []string{"more modules", "images"},
			skippedCount: -1,
		},
		{
			name: "dry-run, security unavailable",
			summary: &mirror.PullSummary{
				DryRun:    true,
				Elapsed:   22 * time.Second,
				Platform:  mirror.ComponentStats{Attempted: true, Images: 412},
				Installer: mirror.ComponentStats{Attempted: true, Images: 1},
				Security:  mirror.SecurityStats{Attempted: true, Available: false, AvailableDatabases: 4},
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 356,
					Modules:     []mirror.ModuleStat{{Name: "commander", Images: 18}},
				},
			},
			contains: []string{
				"Pull plan (dry-run)",
				"not available in this edition",
				"No images were downloaded (dry-run).",
			},
			notContains:  []string{"Bundle artifacts", "databases", "4/4"},
			skippedCount: -1,
		},
		{
			name: "skipped categories",
			summary: &mirror.PullSummary{
				Platform:  mirror.ComponentStats{Skipped: true},
				Installer: mirror.ComponentStats{Attempted: true, Images: 2},
				Security:  mirror.SecurityStats{Skipped: true},
				Modules:   mirror.ModulesStats{Skipped: true},
			},
			contains: []string{
				"Platform:", "Security:", "Modules:",
				"Installer:", "included",
			},
			skippedCount: 3, // Platform, Security, Modules
		},
		{
			name: "zero modules attempted",
			summary: &mirror.PullSummary{
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 0,
					Modules:     []mirror.ModuleStat{},
				},
			},
			contains:     []string{"0 modules"},
			notContains:  []string{"skipped", "more modules", "images"},
			skippedCount: 0,
		},
		{
			name: "vex present, verbose shows per-module VEX",
			summary: &mirror.PullSummary{
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 100,
					TotalVEX:    12,
					Modules: []mirror.ModuleStat{
						{Name: "commander", Images: 57, VEX: 12},
						{Name: "console", Images: 43, VEX: 0},
					},
				},
			},
			verbose: true,
			contains: []string{
				"2 modules", "12 VEXes",
				"commander", "(12 VEX)",
			},
			// console has 0 VEX -> no per-module VEX note for it.
			notContains:  []string{"(0 VEX)", "images", "including"},
			skippedCount: -1,
		},
		{
			name: "vex absent omits the parenthetical",
			summary: &mirror.PullSummary{
				Modules: mirror.ModulesStats{
					Attempted:   true,
					TotalImages: 100,
					TotalVEX:    0,
					Modules:     []mirror.ModuleStat{{Name: "commander", Images: 57}},
				},
			},
			verbose:      true,
			contains:     []string{"1 modules"},
			notContains:  []string{"VEX", "including", "images"},
			skippedCount: -1,
		},
		{
			name: "only extra images",
			summary: &mirror.PullSummary{
				Modules: mirror.ModulesStats{
					Attempted:       true,
					OnlyExtraImages: true,
					TotalImages:     12,
					Modules:         []mirror.ModuleStat{{Name: "commander", Images: 12}},
				},
			},
			contains:     []string{"1 modules", "extra images only"},
			skippedCount: -1,
		},
		{
			name: "cancelled partial",
			summary: &mirror.PullSummary{
				Cancelled: true,
				Elapsed:   3 * time.Minute,
				Platform:  mirror.ComponentStats{Attempted: true, Images: 10, Versions: []string{"v1.69.0"}},
				// Installer / Security / Modules never ran -> "not pulled".
			},
			contains: []string{
				"Pull was cancelled; the above reflects what completed.",
				"not pulled",
			},
			// A phase that never ran must not masquerade as available/empty.
			notContains:  []string{"not available in this edition", "0 modules"},
			skippedCount: -1,
		},
		{
			name: "hard failure renders partial summary in failed state",
			summary: &mirror.PullSummary{
				Failed:   true,
				Elapsed:  5 * time.Minute,
				Platform: mirror.ComponentStats{Attempted: true, Versions: []string{"v1.69.0"}, Channels: []string{"stable"}},
				// Installer / Security / Modules never ran -> "not pulled".
			},
			contains: []string{
				"Pull failed",
				"v1.69.0",
				"not pulled",
				"Pull failed; the above reflects what completed before the error.",
			},
			notContains:  []string{"Pull summary", "included", "not available in this edition"},
			skippedCount: -1,
		},
		{
			name: "cancelled during dry-run shows cancellation, not the dry-run footer",
			summary: &mirror.PullSummary{
				DryRun:    true,
				Cancelled: true,
				Elapsed:   2 * time.Second,
				Platform:  mirror.ComponentStats{Attempted: true, Versions: []string{"v1.69.0"}},
			},
			// Ctrl+C during a dry-run sets both flags; cancellation must win the footer.
			contains:     []string{"Pull was cancelled; the above reflects what completed."},
			notContains:  []string{"No images were downloaded (dry-run)."},
			skippedCount: -1,
		},
		{
			name: "everything skipped renders four skipped lines and no body",
			summary: &mirror.PullSummary{
				Platform:  mirror.ComponentStats{Skipped: true},
				Installer: mirror.ComponentStats{Skipped: true},
				Security:  mirror.SecurityStats{Skipped: true},
				Modules:   mirror.ModulesStats{Skipped: true},
			},
			contains:     []string{"Platform:", "Installer:", "Security:", "Modules:"},
			notContains:  []string{"Bundle artifacts", "VEX", "not pulled"},
			skippedCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := renderPullSummary(tt.summary, tt.verbose)

			// Sanity: the block is framed.
			require.Contains(t, out, "╔══", "missing top border")
			require.Contains(t, out, "╚", "missing bottom border")

			for _, want := range tt.contains {
				require.Contains(t, out, want)
			}

			for _, notWant := range tt.notContains {
				require.NotContains(t, out, notWant)
			}

			if tt.skippedCount >= 0 {
				require.Equal(t, tt.skippedCount, strings.Count(out, "skipped"))
			}
		})
	}
}

// TestRenderPullSummary_ColorGating verifies that colour is emitted only when
// enabled and fully suppressed otherwise - the contract that keeps escape codes
// out of pipes, files, and captured logs (fatih/color flips color.NoColor from
// the stdout TTY check and NO_COLOR, which the logger writes to).
func TestRenderPullSummary_ColorGating(t *testing.T) {
	orig := color.NoColor
	defer func() { color.NoColor = orig }()

	s := &mirror.PullSummary{
		Security: mirror.SecurityStats{Attempted: true, Available: true, Databases: 4, AvailableDatabases: 4},
		Modules: mirror.ModulesStats{
			Attempted:   true,
			TotalImages: 1,
			TotalVEX:    1,
			Modules:     []mirror.ModuleStat{{Name: "code", Images: 1, VEX: 1, Versions: []string{"v1.0.0"}}},
		},
	}

	color.NoColor = false
	require.Contains(t, renderPullSummary(s, true), "\x1b[",
		"ANSI escape codes must be present when colour is enabled")

	color.NoColor = true
	require.NotContains(t, renderPullSummary(s, true), "\x1b[",
		"no ANSI escape codes when colour is disabled (NO_COLOR / non-TTY / piped)")
}

func TestHumanSize(t *testing.T) {
	cases := map[int64]string{
		0:             "0 B",
		-5:            "-5 B", // negatives fall through the < 1024 branch unchanged
		512:           "512 B",
		1023:          "1023 B",
		1024:          "1.0 KiB",
		1536:          "1.5 KiB",
		1024 * 1024:   "1.0 MiB",
		3_100_000_000: "2.9 GiB",
		1 << 40:       "1.0 TiB",
		1 << 50:       "1.0 PiB",
		1 << 60:       "1.0 EiB", // top of the KMGTPE unit string
	}

	for in, want := range cases {
		require.Equal(t, want, humanSize(in))
	}
}

func TestFormatDuration(t *testing.T) {
	cases := map[time.Duration]string{
		0:                               "0s",
		500 * time.Millisecond:          "500ms", // sub-second keeps ms precision
		time.Second:                     "1s",
		1500 * time.Millisecond:         "2s", // rounds to the nearest second
		14*time.Minute + 32*time.Second: "14m32s",
		1*time.Hour + 47*time.Minute:    "1h47m0s",
	}

	for in, want := range cases {
		require.Equal(t, want, formatDuration(in))
	}
}

func TestSortVersions(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"empty", nil, nil},
		{"single", []string{"v1.2.3"}, []string{"v1.2.3"}},
		{"semver newest-first, numeric not lexical", []string{"v1.9.16", "v1.10.3", "v1.5.4"}, []string{"v1.10.3", "v1.9.16", "v1.5.4"}},
		{"major jump", []string{"v1.10.3", "v10.0.1", "v1.5.4"}, []string{"v10.0.1", "v1.10.3", "v1.5.4"}},
		{"all non-semver sort lexically", []string{"release-b", "release-a", "custom"}, []string{"custom", "release-a", "release-b"}},
		{"mixed: semver first (desc), non-semver after (lexical)", []string{"latest", "v1.2.3", "edge", "v2.0.0"}, []string{"v2.0.0", "v1.2.3", "edge", "latest"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sortVersions(tc.in))
		})
	}
}

// TestConfigureSummaryColor verifies the env-var precedence that re-enables
// colour for intentional piping: NO_COLOR wins over FORCE_COLOR / CLICOLOR_FORCE.
func TestConfigureSummaryColor(t *testing.T) {
	cases := []struct {
		name             string
		noColor          string
		forceColor       string
		cliColorForce    string
		startNoColor     bool
		wantNoColorAfter bool
	}{
		{name: "FORCE_COLOR re-enables colour", forceColor: "1", startNoColor: true, wantNoColorAfter: false},
		{name: "CLICOLOR_FORCE re-enables colour", cliColorForce: "1", startNoColor: true, wantNoColorAfter: false},
		{name: "NO_COLOR wins over FORCE_COLOR", noColor: "1", forceColor: "1", startNoColor: true, wantNoColorAfter: true},
		{name: "no env vars leaves it untouched", startNoColor: true, wantNoColorAfter: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NO_COLOR", tc.noColor)
			t.Setenv("FORCE_COLOR", tc.forceColor)
			t.Setenv("CLICOLOR_FORCE", tc.cliColorForce)

			orig := color.NoColor
			defer func() { color.NoColor = orig }()

			color.NoColor = tc.startNoColor
			configureSummaryColor()
			require.Equal(t, tc.wantNoColorAfter, color.NoColor)
		})
	}
}

func TestPhysicalFileCount(t *testing.T) {
	cases := []struct {
		name  string
		files []mirror.BundleFile
		want  int
	}{
		{"no files", nil, 0},
		{"single tar counts as one", []mirror.BundleFile{{Name: "platform.tar"}}, 1},
		{"chunks=1 counts as one chunk", []mirror.BundleFile{{Name: "a.tar", Chunks: 1}}, 1},
		{
			name: "mix of single and chunked",
			files: []mirror.BundleFile{
				{Name: "platform.tar"},            // single -> 1
				{Name: "module-a.tar", Chunks: 3}, // chunked -> 3
				{Name: "installer.tar"},           // single -> 1
			},
			want: 5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, physicalFileCount(mirror.BundleStats{Files: tc.files}))
		})
	}
}
