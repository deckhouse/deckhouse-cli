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

	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	safereq "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// helper to create SafeClient with empty rest.Config (no auth)
func newNoAuthSafe() *safereq.SafeClient {
	// Ensure that SafeClient allows unauthenticated HTTP requests during unit tests.
	safereq.SupportNoAuth = true
	// Temporarily set KUBECONFIG to /dev/null to avoid loading auth from kubeconfig
	oldKubeconfig := os.Getenv("KUBECONFIG")
	os.Setenv("KUBECONFIG", "/dev/null")
	defer os.Setenv("KUBECONFIG", oldKubeconfig)
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
	origPrep := util.PrepareDownloadFunc
	origCreate := util.CreateDataExporterIfNeededFunc
	util.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/files", "Filesystem", newNoAuthSafe(), nil
	}
	util.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() {
		util.PrepareDownloadFunc = origPrep
		util.CreateDataExporterIfNeededFunc = origCreate
	}()

	outFile := filepath.Join(t.TempDir(), "out.txt")

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "foo.txt", "-o", outFile, "--publish=false"})
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

	origPrep := util.PrepareDownloadFunc
	origCreate := util.CreateDataExporterIfNeededFunc
	util.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/files", "Block", newNoAuthSafe(), nil
	}
	util.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { util.PrepareDownloadFunc = origPrep; util.CreateDataExporterIfNeededFunc = origCreate }()

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "foo.txt", "-o", filepath.Join(t.TempDir(), "out.txt"), "--publish=false"})
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

	origPrep := util.PrepareDownloadFunc
	origCreate := util.CreateDataExporterIfNeededFunc
	util.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/block", "Block", newNoAuthSafe(), nil
	}
	util.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() {
		util.PrepareDownloadFunc = origPrep
		util.CreateDataExporterIfNeededFunc = origCreate
	}()

	outFile := filepath.Join(t.TempDir(), "raw.img")
	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "-o", outFile, "--publish=false"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	require.NoError(t, cmd.Execute())
	data, err := os.ReadFile(outFile)
	require.NoError(t, err)
	require.Equal(t, []byte("raw!"), data)
}

// Regression: when a directory listing contains an entry with type "other" (socket, FIFO, device),
// the client must skip it with a warning and NOT make an HTTP request for it.
// Before the fix, "other" entries were reported as "dir", causing the client to recurse into them
// and receive a 400 from the server, which aborted the entire download.
func TestDownloadFilesystem_SocketInDirIsSkipped(t *testing.T) {
	requestedPaths := make([]string, 0)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPaths = append(requestedPaths, r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/files/queue/":
			// Directory listing: one regular file + one socket (type "other")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"apiVersion":"v1","items":[` +
				`{"name":"alerts.log","type":"file","uri":"queue/alerts.log","attributes":{"gid":0,"modtime":"2026-01-01T00:00:00Z","permissions":"0644","uid":0,"size":3}},` +
				`{"name":"execq","type":"other","uri":"queue/execq","attributes":{"gid":999,"modtime":"2026-01-01T00:00:00Z","permissions":"0660","uid":0}}` +
				`]}`))
		case "/api/v1/files/queue/alerts.log":
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok\n"))
		default:
			// Fail loudly if client requests unexpected paths (e.g. the socket)
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	origPrep := util.PrepareDownloadFunc
	origCreate := util.CreateDataExporterIfNeededFunc
	util.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/files", "Filesystem", newNoAuthSafe(), nil
	}
	util.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { util.PrepareDownloadFunc = origPrep; util.CreateDataExporterIfNeededFunc = origCreate }()

	outDir := t.TempDir()
	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "queue/", "-o", outDir, "--publish=false"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	require.NoError(t, cmd.Execute())

	// Regular file must be downloaded
	data, err := os.ReadFile(filepath.Join(outDir, "alerts.log"))
	require.NoError(t, err)
	require.Equal(t, []byte("ok\n"), data)

	// Socket must NOT have been requested from the server
	for _, p := range requestedPaths {
		require.NotContains(t, p, "execq", "client must not request socket path, got requests: %v", requestedPaths)
	}
}

// Regression: recursive download through a directory tree that includes sockets
// must complete successfully and download all regular files.
func TestDownloadFilesystem_RecursiveWithSocketsCompletes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/files/root/":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"apiVersion":"v1","items":[` +
				`{"name":"subdir","type":"dir","uri":"root/subdir/","attributes":{"gid":0,"modtime":"2026-01-01T00:00:00Z","permissions":"0755","uid":0}},` +
				`{"name":"top.txt","type":"file","uri":"root/top.txt","attributes":{"gid":0,"modtime":"2026-01-01T00:00:00Z","permissions":"0644","uid":0,"size":3}}` +
				`]}`))
		case "/api/v1/files/root/subdir/":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// subdir contains a socket and a regular file
			w.Write([]byte(`{"apiVersion":"v1","items":[` +
				`{"name":"cfgarq","type":"other","uri":"root/subdir/cfgarq","attributes":{"gid":999,"modtime":"2026-01-01T00:00:00Z","permissions":"0660","uid":0}},` +
				`{"name":"data.txt","type":"file","uri":"root/subdir/data.txt","attributes":{"gid":0,"modtime":"2026-01-01T00:00:00Z","permissions":"0644","uid":0,"size":5}}` +
				`]}`))
		case "/api/v1/files/root/top.txt":
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("top"))
		case "/api/v1/files/root/subdir/data.txt":
			w.Header().Set("Content-Length", "5")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("inner"))
		default:
			http.Error(w, "unexpected: "+r.URL.Path, http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	origPrep := util.PrepareDownloadFunc
	origCreate := util.CreateDataExporterIfNeededFunc
	util.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/files", "Filesystem", newNoAuthSafe(), nil
	}
	util.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { util.PrepareDownloadFunc = origPrep; util.CreateDataExporterIfNeededFunc = origCreate }()

	outDir := t.TempDir()
	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "root/", "-o", outDir, "--publish=false"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	require.NoError(t, cmd.Execute())

	// Both regular files must be present
	top, err := os.ReadFile(filepath.Join(outDir, "top.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("top"), top)

	inner, err := os.ReadFile(filepath.Join(outDir, "subdir", "data.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("inner"), inner)

	// Socket must NOT exist on disk
	_, err = os.Stat(filepath.Join(outDir, "subdir", "cfgarq"))
	require.True(t, os.IsNotExist(err), "socket must not be created on disk")
}

func TestDownloadBlock_WrongEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "VolumeMode: Filesystem. Not supported downloading raw block.", http.StatusBadRequest)
	}))
	defer srv.Close()

	origPrep := util.PrepareDownloadFunc
	origCreate := util.CreateDataExporterIfNeededFunc
	util.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return srv.URL + "/api/v1/block", "Filesystem", newNoAuthSafe(), nil
	}
	util.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}
	defer func() { util.PrepareDownloadFunc = origPrep; util.CreateDataExporterIfNeededFunc = origCreate }()

	cmd := NewCommand(context.TODO(), slog.Default())
	cmd.SetArgs([]string{"myexport", "-o", filepath.Join(t.TempDir(), "raw.img"), "--publish=false"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	require.NoError(t, cmd.Execute())
}
