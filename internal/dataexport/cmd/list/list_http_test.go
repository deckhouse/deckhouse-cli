package list

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	datautil "github.com/deckhouse/deckhouse-cli/internal/dataexport/util"
	safereq "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

func newSafe() *safereq.SafeClient {
	// Ensure that SafeClient allows unauthenticated HTTP requests during unit tests.
	safereq.SupportNoAuth = true
	// Create a temporary empty kubeconfig to prevent loading real auth
	tmpDir := os.TempDir()
	emptyKubeconfig := filepath.Join(tmpDir, "empty-kubeconfig")
	os.WriteFile(emptyKubeconfig, []byte(""), 0644)
	defer os.Remove(emptyKubeconfig)

	// Set KUBECONFIG to empty file to prevent loading real kubeconfig
	oldKubeconfig := os.Getenv("KUBECONFIG")
	os.Setenv("KUBECONFIG", emptyKubeconfig)
	defer func() {
		if oldKubeconfig != "" {
			os.Setenv("KUBECONFIG", oldKubeconfig)
		} else {
			os.Unsetenv("KUBECONFIG")
		}
	}()

	sc, _ := safereq.NewSafeClient()
	return sc.Copy()
}

func TestListFilesystem_OK(t *testing.T) {
	// JSON listing for root dir
	respBody := `{"apiVersion":"v1","items":[{"name":"file.txt","type":"file"}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/files/", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(respBody))
	}))
	defer srv.Close()

	origPrep := datautil.PrepareDownloadFunc
	origCreate := datautil.CreateDataExporterIfNeededFunc
	datautil.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		// Re-enable support for unauthenticated requests inside unit tests.
		safereq.SupportNoAuth = true
		return srv.URL + "/api/v1/files", "Filesystem", newSafe(), nil
	}
	datautil.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { datautil.PrepareDownloadFunc = origPrep; datautil.CreateDataExporterIfNeededFunc = origCreate }()

	oldStd := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "/"})
	require.NoError(t, cmd.Execute())

	w.Close()
	var bufOut bytes.Buffer
	io.Copy(&bufOut, r)
	os.Stdout = oldStd

	require.Contains(t, bufOut.String(), "file.txt")
}

func TestListBlock_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodHead, r.Method)
		w.Header().Set("Content-Length", "1234")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	origPrep := datautil.PrepareDownloadFunc
	origCreate := datautil.CreateDataExporterIfNeededFunc
	datautil.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		// Re-enable support for unauthenticated requests inside unit tests.
		safereq.SupportNoAuth = true
		return srv.URL + "/api/v1/block", "Block", newSafe(), nil
	}
	datautil.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { datautil.PrepareDownloadFunc = origPrep; datautil.CreateDataExporterIfNeededFunc = origCreate }()

	// capture stdout because list writes to Stdout directly
	oldStd := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport"})
	require.NoError(t, cmd.Execute())

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stdout = oldStd

	require.Contains(t, buf.String(), "Disk size:")
}

func TestListFilesystem_NotDir(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not dir", http.StatusBadRequest)
	}))
	defer srv.Close()

	origPrep := datautil.PrepareDownloadFunc
	origCreate := datautil.CreateDataExporterIfNeededFunc
	datautil.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		// Re-enable support for unauthenticated requests inside unit tests.
		safereq.SupportNoAuth = true
		return srv.URL + "/api/v1/files", "Filesystem", newSafe(), nil
	}
	datautil.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { datautil.PrepareDownloadFunc = origPrep; datautil.CreateDataExporterIfNeededFunc = origCreate }()

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"myexport", "some/invalid"})
	err := cmd.Execute()
	require.Error(t, err)
}
