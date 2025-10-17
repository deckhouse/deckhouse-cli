package download

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

// helper to create SafeClient with empty rest.Config (no auth)
func newNoAuthSafe() *safereq.SafeClient {
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

func TestDownloadFilesystem_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/files/foo.txt", r.URL.Path)
		w.Header().Set("X-Type", "file")
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("abc"))
	}))
	defer srv.Close()

	// stub PrepareDownload / CreateDataExporterIfNeeded
	origPrep := datautil.PrepareDownloadFunc
	origCreate := datautil.CreateDataExporterIfNeededFunc
	datautil.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/files", "Filesystem", newNoAuthSafe(), nil
	}
	datautil.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() {
		datautil.PrepareDownloadFunc = origPrep
		datautil.CreateDataExporterIfNeededFunc = origCreate
	}()

	outFile := filepath.Join(t.TempDir(), "out.txt")

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "foo.txt", "-o", outFile})
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)

	require.NoError(t, cmd.Execute())

	data, err := os.ReadFile(outFile)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), data)
}

func TestDownloadFilesystem_BadPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate Block-mode error when files endpoint is used
		http.Error(w, "VolumeMode: Block. Not supported downloading files.", http.StatusBadRequest)
	}))
	defer srv.Close()

	origPrep := datautil.PrepareDownloadFunc
	origCreate := datautil.CreateDataExporterIfNeededFunc
	datautil.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/files", "Block", newNoAuthSafe(), nil
	}
	datautil.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { datautil.PrepareDownloadFunc = origPrep; datautil.CreateDataExporterIfNeededFunc = origCreate }()

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "foo.txt", "-o", filepath.Join(t.TempDir(), "out.txt")})
	require.NoError(t, cmd.Execute())
}

func TestDownloadBlock_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/block", r.URL.Path)
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("raw!"))
	}))
	defer srv.Close()

	origPrep := datautil.PrepareDownloadFunc
	origCreate := datautil.CreateDataExporterIfNeededFunc
	datautil.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/block", "Block", newNoAuthSafe(), nil
	}
	datautil.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() {
		datautil.PrepareDownloadFunc = origPrep
		datautil.CreateDataExporterIfNeededFunc = origCreate
	}()

	outFile := filepath.Join(t.TempDir(), "raw.img")
	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "-o", outFile})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	require.NoError(t, cmd.Execute())
	data, err := os.ReadFile(outFile)
	require.NoError(t, err)
	require.Equal(t, []byte("raw!"), data)
}

func TestDownloadBlock_WrongEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "VolumeMode: Filesystem. Not supported downloading raw block.", http.StatusBadRequest)
	}))
	defer srv.Close()

	origPrep := datautil.PrepareDownloadFunc
	origCreate := datautil.CreateDataExporterIfNeededFunc
	datautil.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/block", "Filesystem", newNoAuthSafe(), nil
	}
	datautil.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { datautil.PrepareDownloadFunc = origPrep; datautil.CreateDataExporterIfNeededFunc = origCreate }()

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "-o", filepath.Join(t.TempDir(), "raw.img")})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	require.NoError(t, cmd.Execute())
}
