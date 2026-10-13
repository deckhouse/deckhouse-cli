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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/require"

	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
)

func TestPullCommandSinceVersion(t *testing.T) {
	t.Setenv("STUB_REGISTRY_CLIENT", "true")
	for _, key := range []string{
		"D8_MIRROR_LICENSE_TOKEN", "D8_MIRROR_SOURCE_LOGIN", "D8_MIRROR_SOURCE_PASSWORD",
		"D8_MIRROR_TIMEOUT", "MIRROR_DEBUG_LOG",
	} {
		t.Setenv(key, "")
	}
	noColor := color.NoColor
	color.NoColor = true
	t.Cleanup(func() { color.NoColor = noColor })

	const baseline = "v1.72.10, v1.71.0, v1.70.0, v1.69.0, v1.68.0 (5 channels)"
	const withOlderRelease = "v1.72.10, v1.71.0, v1.70.0, v1.69.0, v1.68.0, v1.67.0 (5 channels)"

	tests := []struct {
		name         string
		args         []string
		wantPlatform string
		wantError    string
	}{
		{
			name:         "without since excludes releases below Rock Solid",
			wantPlatform: baseline,
		},
		{
			name:         "older since includes requested release but not earlier releases",
			args:         []string{"--since-version", "1.67.0"},
			wantPlatform: withOlderRelease,
		},
		{
			name:         "since equal to Rock Solid preserves versions and channels",
			args:         []string{"--since-version", "1.68.0"},
			wantPlatform: baseline,
		},
		{
			name:         "since newer than Rock Solid preserves versions and channels",
			args:         []string{"--since-version", "1.70.0"},
			wantPlatform: baseline,
		},
		{
			name:         "since equal to Alpha preserves versions and channels",
			args:         []string{"--since-version", "1.72.10"},
			wantPlatform: baseline,
		},
		{
			name:         "include platform selects older release",
			args:         []string{"--include-platform", ">=1.67.0"},
			wantPlatform: withOlderRelease,
		},
		{
			name:         "deckhouse tag selects only the requested release",
			args:         []string{"--deckhouse-tag", "v1.67.0"},
			wantPlatform: "v1.67.0",
		},
		{
			name:      "since conflicts with deckhouse tag",
			args:      []string{"--since-version", "1.67.0", "--deckhouse-tag", "v1.67.0"},
			wantError: "Using both --deckhouse-tag and --since-version",
		},
		{
			name:      "since conflicts with include platform",
			args:      []string{"--since-version", "1.67.0", "--include-platform", ">=1.67.0"},
			wantError: "Using both --since-version and --include-platform",
		},
		{
			name:      "since conflicts with proxy registry",
			args:      []string{"--since-version", "1.67.0", "--proxy-registry"},
			wantError: "--proxy-registry cannot be combined with --since-version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := executeDryRunCommand(t, tt.args)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err, output)
			require.Contains(t, output, "Pull plan (dry-run)")

			var platformLines []string
			for _, line := range strings.Split(output, "\n") {
				if _, content, ok := strings.Cut(line, "Platform:"); ok {
					platformLines = append(platformLines, strings.TrimSpace(content))
				}
			}
			require.Equal(t, []string{tt.wantPlatform}, platformLines, output)
		})
	}
}

func executeDryRunCommand(t *testing.T, extraArgs []string) (string, error) {
	t.Helper()
	defer saveFlagsAndRestore(t)()
	pullflags.SinceVersion = nil
	pullflags.PlatformConstraint = nil

	dir := t.TempDir()
	output, err := os.Create(filepath.Join(dir, "output.log"))
	require.NoError(t, err)
	defer output.Close()

	stdout := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = stdout }()

	cmd := NewCommand()
	cmd.SetOut(output)
	cmd.SetErr(output)
	cmd.SetArgs(append([]string{
		filepath.Join(dir, "bundle"),
		"--source", "registry.deckhouse.ru/deckhouse/fe",
		"--tmp-dir", filepath.Join(dir, "work"),
		"--dry-run", "--no-modules", "--no-packages", "--no-installer",
		"--no-security-db", "--skip-vex-images", "--gost-digest=false",
	}, extraArgs...))
	commandErr := cmd.ExecuteContext(context.Background())

	body, err := os.ReadFile(output.Name())
	require.NoError(t, err)
	return string(body), commandErr
}
