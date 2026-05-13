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

package imagefs

import (
	"path"
	"strings"
)

const (
	whiteoutPrefix       = ".wh."
	whiteoutOpaqueMarker = ".wh..wh..opq"
)

// Whiteout classifies a tar entry name as:
//   - opaque marker (whole directory deleted from lower layers): returns (dir, true)
//   - regular whiteout (single entry deleted): returns (targetPath, false)
//   - not a whiteout: returns ("", false)
func Whiteout(name string) (string, bool) {
	base := path.Base(name)
	dir := path.Dir(name)

	if base == whiteoutOpaqueMarker {
		if dir == "." || dir == "/" {
			return ".", true
		}
		return strings.TrimPrefix(dir, "./"), true
	}
	if target, ok := strings.CutPrefix(base, whiteoutPrefix); ok {
		// Reject malformed markers whose suffix is empty (".wh.") or just
		// a dot (".wh..") - both would resolve to the directory itself
		// and, without this guard, instruct applyWhiteout to RemoveAll
		// the whiteout's parent directory.
		if target == "" || target == "." {
			return "", false
		}
		if dir == "." || dir == "/" {
			return target, false
		}
		return path.Join(strings.TrimPrefix(dir, "./"), target), false
	}
	return "", false
}
