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

import "testing"

func TestWhiteout(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantTarget string
		wantOpaque bool
	}{
		{"plain file is not whiteout", "etc/passwd", "", false},
		{"root whiteout", ".wh.foo", "foo", false},
		{"nested whiteout", "bin/.wh.sh", "bin/sh", false},
		{"deep whiteout", "usr/local/bin/.wh.npm", "usr/local/bin/npm", false},
		{"opaque at root", ".wh..wh..opq", ".", true},
		{"opaque in dir", "var/log/.wh..wh..opq", "var/log", true},
		{"regular file with .wh in name", "foo.wh.bar", "", false},
		{"empty string", "", "", false},
		{"malformed marker with empty target", ".wh.", "", false},
		{"malformed marker with dot target", ".wh..", "", false},
		{"malformed marker with empty target in subdir", "subdir/.wh.", "", false},
		{"malformed marker with dot target in subdir", "subdir/.wh..", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, opaque := Whiteout(c.input)
			if target != c.wantTarget || opaque != c.wantOpaque {
				t.Errorf("Whiteout(%q) = (%q, %v), want (%q, %v)",
					c.input, target, opaque, c.wantTarget, c.wantOpaque)
			}
		})
	}
}
