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
	"slices"
	"testing"
)

func TestFilterBySubpath_StrictScope(t *testing.T) {
	entries := []Entry{
		{Path: "etc", Type: TypeDir},
		{Path: "etc/passwd", Type: TypeFile},
		{Path: "etc/ssh/sshd_config", Type: TypeFile},
		{Path: "usr/bin/passwd", Type: TypeFile},
		{Path: "passwd", Type: TypeFile},
	}

	got := FilterBySubpath(entries, "passwd")
	paths := make([]string, 0, len(got))
	for _, e := range got {
		paths = append(paths, e.Path)
	}
	if !slices.Equal(paths, []string{"passwd"}) {
		t.Fatalf("strict scope expected only root passwd, got: %v", paths)
	}

	got = FilterBySubpath(entries, "etc")
	paths = paths[:0]
	for _, e := range got {
		paths = append(paths, e.Path)
	}
	if !slices.Equal(paths, []string{"etc", "etc/passwd", "etc/ssh/sshd_config"}) {
		t.Fatalf("strict scope for etc mismatch, got: %v", paths)
	}
}

func TestNormalizeScopePath(t *testing.T) {
	cases := map[string]string{
		"":                ".",
		"/":               ".",
		"./":              ".",
		"etc/":            "etc",
		"/etc/passwd":     "etc/passwd",
		"./etc/../etc":    "etc",
		"  /var/log/../ ": "var",
	}
	for in, want := range cases {
		if got := NormalizeScopePath(in); got != want {
			t.Fatalf("NormalizeScopePath(%q) = %q, want %q", in, got, want)
		}
	}
}
