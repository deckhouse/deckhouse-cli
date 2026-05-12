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

// FilterBySubpath returns entries whose tar-relative path is at or under
// subpath. An empty or "."-equivalent subpath returns the entries unchanged.
func FilterBySubpath(entries []Entry, subpath string) []Entry {
	if subpath == "" {
		return entries
	}
	sub := NormalizeScopePath(subpath)
	if sub == "." {
		return entries
	}
	prefix := sub + "/"
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if e.Path == sub || strings.HasPrefix(e.Path, prefix) {
			out = append(out, e)
		}
	}
	return out
}

// NormalizeScopePath canonicalizes a user-supplied PATH argument used to
// scope `fs ls`/`fs tree` output. Leading "./" or "/" is stripped, the
// path is Clean'd, trailing whitespace is trimmed, and an empty result
// is normalized to ".".
func NormalizeScopePath(raw string) string {
	v := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(raw), "./"), "/")
	if v == "" {
		return "."
	}
	return strings.TrimPrefix(path.Clean(v), "./")
}
