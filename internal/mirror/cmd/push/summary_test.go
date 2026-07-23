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

package push

import (
	"testing"
	"time"

	"github.com/fatih/color"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
)

func TestRenderPushSummary(t *testing.T) {
	color.NoColor = true

	const root = "registry.example.com/deckhouse/ee"

	tests := []struct {
		name        string
		summary     *mirror.PushSummary
		contains    []string
		notContains []string
	}{
		{
			name: "default layout lists every component",
			summary: &mirror.PushSummary{
				Registry:          mirror.BuildRegistryLayout(root, "/modules", root+"/installer"),
				PlatformPushed:    true,
				InstallerPushed:   true,
				SecurityDatabases: 4,
				Modules:           12,
				Packages:          3,
				Elapsed:           2*time.Minute + 4*time.Second,
			},
			contains: []string{
				"Push summary",
				"Registry:", root + "/modules",
				"Platform:", "pushed",
				"Security:", "4 databases",
				"Modules:", "12",
				"Packages:", "3",
				"Elapsed: 2m4s",
			},
			notContains: []string{"default:", "failed", "cancelled", "not present"},
		},
		{
			name: "moved modules path is highlighted with a default hint",
			summary: &mirror.PushSummary{
				Registry:       mirror.BuildRegistryLayout(root, "/", root+"/installer"),
				PlatformPushed: true,
				Modules:        5,
			},
			contains: []string{
				"Registry:",
				"default: " + root + "/modules",
				"Installer:", "not present", // no installer.tar in this push
				"Modules:", "5",
			},
		},
		{
			name: "failed push renders a FAILED state",
			summary: &mirror.PushSummary{
				Registry: mirror.BuildRegistryLayout(root, "/modules", root+"/installer"),
				Failed:   true,
			},
			contains: []string{"Push failed", "Push failed; the above reflects what completed"},
		},
		{
			name: "cancelled push renders a cancellation state",
			summary: &mirror.PushSummary{
				Registry:  mirror.BuildRegistryLayout(root, "/modules", root+"/installer"),
				Cancelled: true,
			},
			contains: []string{"Push was cancelled; the above reflects what completed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := renderPushSummary(tt.summary)

			require.Contains(t, out, "╔══", "missing top border")
			require.Contains(t, out, "╚", "missing bottom border")

			for _, want := range tt.contains {
				require.Contains(t, out, want)
			}

			for _, notWant := range tt.notContains {
				require.NotContains(t, out, notWant)
			}
		})
	}
}
