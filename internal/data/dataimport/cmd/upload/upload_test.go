package upload

import (
	"context"
	"log/slog"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadCommand_FlagsParse(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	tests := []struct {
		name string
		args []string
		exp  struct {
			name, ns, file, dst string
			chunks              int
			publish, resume     bool
		}
	}{
		{
			name: "resume + chunks",
			args: []string{"test-dataimport", "-n", "di-test", "-P", "-d", "/dst/path", "-f", "./test-file", "-c", "4", "--resume"},
			exp: struct {
				name, ns, file, dst string
				chunks              int
				publish, resume     bool
			}{name: "test-dataimport", ns: "di-test", file: "./test-file", dst: "/dst/path", chunks: 4, publish: true, resume: true},
		},
		{
			name: "defaults",
			args: []string{"di-name", "-n", "ns", "-P", "-d", "/dst", "-f", "file"},
			exp: struct {
				name, ns, file, dst string
				chunks              int
				publish, resume     bool
			}{name: "di-name", ns: "ns", file: "file", dst: "/dst", chunks: 10, publish: true, resume: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewCommand(ctx, logger)
			cmd.SetArgs(tt.args)

			var got struct {
				name, ns, file, dst string
				chunks              int
				publish, resume     bool
			}

			orig := cmd.RunE
			t.Cleanup(func() { cmd.RunE = orig })

			cmd.RunE = func(c *cobra.Command, args []string) error {
				got.name = args[0]
				got.ns, _ = c.Flags().GetString("namespace")
				got.file, _ = c.Flags().GetString("file")
				got.dst, _ = c.Flags().GetString("dstPath")
				got.chunks, _ = c.Flags().GetInt("chunks")
				got.publish, _ = c.Flags().GetBool("publish")
				got.resume, _ = c.Flags().GetBool("resume")
				return nil
			}

			require.NoError(t, cmd.Execute())
			assert.Equal(t, tt.exp.name, got.name)
			assert.Equal(t, tt.exp.ns, got.ns)
			assert.Equal(t, tt.exp.file, got.file)
			assert.Equal(t, tt.exp.dst, got.dst)
			assert.Equal(t, tt.exp.chunks, got.chunks)
			assert.Equal(t, tt.exp.publish, got.publish)
			assert.Equal(t, tt.exp.resume, got.resume)
		})
	}
}
