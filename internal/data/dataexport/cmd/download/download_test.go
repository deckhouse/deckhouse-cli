package download

import (
	"testing"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/stretchr/testify/require"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name      string
		input     []string
		wantDe    string
		wantPath  string
		wantError bool
	}{
		{
			name:     "name only",
			input:    []string{"my-export"},
			wantDe:   "my-export",
			wantPath: "/",
		},
		{
			name:     "name and path",
			input:    []string{"vd/mydisk", "file.txt"},
			wantDe:   "vd/mydisk",
			wantPath: "/file.txt",
		},
		{
			name:      "too many args",
			input:     []string{"a", "b", "c"},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			de, path, err := dataio.ParseArgs(tt.input)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantDe, de)
			require.Equal(t, tt.wantPath, path)
		})
	}
}
