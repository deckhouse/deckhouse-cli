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

package output

import (
	"fmt"
	"io"

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imagefs"
)

// WriteEntriesText writes entries in simple or long (-l) format.
// Directory entries are emitted with a trailing "/" so the type is visible
// at a glance (matching the convention used by `ls -p`, `tree`, etc.).
func WriteEntriesText(w io.Writer, entries []imagefs.Entry, long bool) error {
	for _, e := range entries {
		path := e.Path
		if e.IsDir() {
			path += "/"
		}
		if long {
			if _, err := fmt.Fprintf(w, "%-11s %8s  %s\n", e.ModeStr, HumanSize(e.Size), path); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintln(w, path); err != nil {
			return err
		}
	}
	return nil
}

// HumanSize returns a compact human-readable size (e.g. "789 B", "12.3 KB").
func HumanSize(n int64) string {
	const unit = int64(1024)
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := unit, 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	units := "KMGTPE"
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), units[exp])
}
