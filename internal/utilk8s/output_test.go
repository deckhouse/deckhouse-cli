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

package utilk8s

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestGetOutputFormat(t *testing.T) {
	tests := []struct {
		name     string
		formats  []string
		args     []string
		expected string
		wantErr  string
	}{
		{
			name:     "default format when flag is not set",
			formats:  []string{"yaml", "json"},
			args:     []string{},
			expected: "yaml",
		},
		{
			name:     "explicit allowed format",
			formats:  []string{"yaml", "json"},
			args:     []string{"-o", "json"},
			expected: "json",
		},
		{
			name:    "unsupported format",
			formats: []string{"yaml", "json"},
			args:    []string{"-o", "xml"},
			wantErr: `unsupported output format "xml"; use yaml|json`,
		},
		{
			name:    "flag not registered",
			formats: nil,
			args:    []string{},
			wantErr: "reading output flag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: "test"}
			if tt.formats != nil {
				AddOutputFlag(cmd, tt.formats[0], tt.formats...)
			}
			assert.NoError(t, cmd.Flags().Parse(tt.args))

			format, err := GetOutputFormat(cmd, tt.formats...)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, format)
		})
	}
}
