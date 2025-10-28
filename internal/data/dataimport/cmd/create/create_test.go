package create

import (
	"context"
	"log/slog"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCommand_FlagsParse(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	tests := []struct {
		name   string
		args   []string
		expect struct {
			name, ns, ttl, file string
			publish, wffc       bool
		}
	}{
		{
			name: "all flags true",
			args: []string{
				"test-dataimport",
				"-n", "di-test",
				"--ttl", "60m",
				"--publish",
				"--wffc=true",
				"-f", "pvctemplate.yaml",
			},
			expect: struct {
				name, ns, ttl, file string
				publish, wffc       bool
			}{name: "test-dataimport", ns: "di-test", ttl: "60m", file: "pvctemplate.yaml", publish: true, wffc: true},
		},
		{
			name: "minimal flags",
			args: []string{
				"di-name",
				"-n", "ns",
				"-f", "pvc.yaml",
			},
			expect: struct {
				name, ns, ttl, file string
				publish, wffc       bool
			}{name: "di-name", ns: "ns", ttl: "2m", file: "pvc.yaml", publish: false, wffc: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewCommand(ctx, logger)
			cmd.SetArgs(tt.args)

			var got struct {
				name, ns, ttl, file string
				publish, wffc       bool
			}

			orig := cmd.RunE
			t.Cleanup(func() { cmd.RunE = orig })

			cmd.RunE = func(c *cobra.Command, args []string) error {
				got.name = args[0]
				got.ns, _ = c.Flags().GetString("namespace")
				got.ttl, _ = c.Flags().GetString("ttl")
				got.publish, _ = c.Flags().GetBool("publish")
				got.wffc, _ = c.Flags().GetBool("wffc")
				got.file, _ = c.Flags().GetString("file")
				return nil
			}

			require.NoError(t, cmd.Execute())
			assert.Equal(t, tt.expect.name, got.name)
			assert.Equal(t, tt.expect.ns, got.ns)
			// ttl has default 2m in command flags
			if tt.expect.ttl != "" {
				assert.Equal(t, tt.expect.ttl, got.ttl)
			}
			assert.Equal(t, tt.expect.publish, got.publish)
			assert.Equal(t, tt.expect.wffc, got.wffc)
			assert.Equal(t, tt.expect.file, got.file)
		})
	}
}
