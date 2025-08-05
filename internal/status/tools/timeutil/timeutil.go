/*
Copyright 2025 Flant JSC

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

package timeutil

import (
	"fmt"
	"time"
)

// AgeAgo formats creation/status transition time (in the style of "2d 3h", "5m", etc.)
func AgeAgo(t time.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	duration := time.Since(t)
	if duration < 0 {
		duration = 0
	}
	return formatDuration(duration)
}

// AgeAgoStr parses a RFC3339 time string and formats it as AgeAgo
func AgeAgoStr(ts string) string {
	if ts == "" {
		return "<unknown>"
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return "Parse Error"
	}
	return AgeAgo(t)
}

// formatDuration formats the output as ("2d 3h", "5m 7s")
func formatDuration(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	switch {
	case days > 0:
		if hours > 0 {
			return fmt.Sprintf("%dd %dh", days, hours)
		}
		return fmt.Sprintf("%dd", days)
	case hours > 0:
		if minutes > 0 {
			return fmt.Sprintf("%dh %dm", hours, minutes)
		}
		return fmt.Sprintf("%dh", hours)
	case minutes > 0:
		if seconds > 0 {
			return fmt.Sprintf("%dm %ds", minutes, seconds)
		}
		return fmt.Sprintf("%dm", minutes)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}
