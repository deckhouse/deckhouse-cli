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

package safepath

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	root := filepath.Join("base", "root")

	tests := []struct {
		give    string
		want    string
		wantErr bool
	}{
		{give: "a/b", want: filepath.Join(root, "a", "b")},
		{give: "dir/foo..bar", want: filepath.Join(root, "dir", "foo..bar")},
		{give: "a/../b", want: filepath.Join(root, "b")},
		{give: "/abs/x", want: filepath.Join(root, "abs", "x")},
		{give: "..", wantErr: true},
		{give: "../x", wantErr: true},
		{give: "a/../../x", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.give, func(t *testing.T) {
			got, err := Join(root, tt.give)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
