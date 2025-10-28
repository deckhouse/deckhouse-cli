package cmd

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

const (
	testName = "exp-test"
	testNS = "default"
	testTTL = "30m"
	testPublish = true
	deprecationWarning = "deprecated"
	testOut = "out.txt"
)

func TestShim_Create_DelegatesToExportCreate(t *testing.T) {
	isCalled := false
	var gotName, gotNS, gotTTL string
	var gotPublish bool
	old := exportCreateRun
	exportCreateRun = func(_ context.Context, _ *slog.Logger, c *cobra.Command, args []string) error {
		isCalled = true
		gotName = args[0]
		gotNS, _ = c.Flags().GetString("namespace")
		gotTTL, _ = c.Flags().GetString("ttl")
		gotPublish, _ = c.Flags().GetBool("publish")
		return nil
	}
	t.Cleanup(func() { exportCreateRun = old })

	root := NewCommand()
	buf := &bytes.Buffer{}
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"create", testName, "pvc/my-pvc", "-n", testNS, "--ttl", testTTL, "--publish"})

	require.NoError(t, root.Execute())
	require.True(t, isCalled)
	require.Equal(t, testName, gotName)
	require.Equal(t, testNS, gotNS)
	require.Equal(t, testTTL, gotTTL)
	require.True(t, testPublish, gotPublish)
	require.Contains(t, buf.String(), deprecationWarning)
}

func TestShim_List_DelegatesToExportList(t *testing.T) {
	isCalled := false
	var gotName, gotNS string
	var gotPublish bool
	old := exportListRun
	exportListRun = func(_ context.Context, _ *slog.Logger, c *cobra.Command, args []string) error {
		isCalled = true
		gotName = args[0]
		gotNS, _ = c.Flags().GetString("namespace")
		gotPublish, _ = c.Flags().GetBool("publish")
		return nil
	}
	t.Cleanup(func() { exportListRun = old })

	root := NewCommand()
	buf := &bytes.Buffer{}
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"list", testName, "-n", testNS, "--publish"})

	require.NoError(t, root.Execute())
	require.True(t, isCalled)
	require.Equal(t, testName, gotName)
	require.Equal(t, testNS, gotNS)
	require.True(t, testPublish, gotPublish)
	require.Contains(t, buf.String(), deprecationWarning)
}

func TestShim_Download_DelegatesToExportDownload(t *testing.T) {
	isCalled := false
	var gotName, gotNS, out string
	var gotPublish bool
	old := exportDownloadRun
	exportDownloadRun = func(_ context.Context, _ *slog.Logger, c *cobra.Command, args []string) error {
		isCalled = true
		gotName = args[0]
		gotNS, _ = c.Flags().GetString("namespace")
		out, _ = c.Flags().GetString("output")
		gotPublish, _ = c.Flags().GetBool("publish")
		return nil
	}
	t.Cleanup(func() { exportDownloadRun = old })

	root := NewCommand()
	buf := &bytes.Buffer{}
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"download", testName, "/file.txt", "-n", testNS, "-o", testOut, "--publish"})
	require.NoError(t, root.Execute())
	require.True(t, isCalled)
	require.Equal(t, testName, gotName)
	require.Equal(t, testNS, gotNS)
	require.Equal(t, testOut, out)
	require.True(t, testPublish, gotPublish)
	require.Contains(t, buf.String(), deprecationWarning)
}

func TestShim_Delete_DelegatesToExportDelete(t *testing.T) {
	isCalled := false
	var gotName, gotNS string
	old := exportDeleteRun
	exportDeleteRun = func(_ context.Context, _ *slog.Logger, c *cobra.Command, args []string) error {
		isCalled = true
		gotName = args[0]
		gotNS, _ = c.Flags().GetString("namespace")
		return nil
	}
	t.Cleanup(func() { exportDeleteRun = old })

	root := NewCommand()
	buf := &bytes.Buffer{}
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"delete", testName, "-n", testNS})

	require.NoError(t, root.Execute())
	require.True(t, isCalled)
	require.Equal(t, testName, gotName)
	require.Equal(t, testNS, gotNS)
	require.Contains(t, buf.String(), deprecationWarning)
}
