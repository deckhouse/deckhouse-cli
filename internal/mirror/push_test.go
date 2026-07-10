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

package mirror

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPackageNameFromPath covers the .tar-only contract packageNameFromPath
// relies on: cmd/push/validation.go always canonicalizes chunked archives to
// their <name>.tar path (see canonicalPackagePath) before they reach
// PushService, so this function only ever needs to strip ".tar".
func TestPackageNameFromPath(t *testing.T) {
	tests := []struct {
		name    string
		pkgPath string
		want    string
	}{
		{
			name:    "absolute tar path",
			pkgPath: "/bundle/platform.tar",
			want:    "platform",
		},
		{
			name:    "relative tar path",
			pkgPath: "platform.tar",
			want:    "platform",
		},
		{
			name:    "module tar path with dashes in the name",
			pkgPath: filepath.Join("/bundle", "module-foo.tar"),
			want:    "module-foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, packageNameFromPath(tt.pkgPath))
		})
	}
}
