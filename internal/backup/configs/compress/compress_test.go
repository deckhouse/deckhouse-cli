package compress

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func createSampleTar(t *testing.T, tarFilePath string) {
	tarFile, err := os.Create(tarFilePath)
	require.NoError(t, err, "failed to create tar file")
	defer tarFile.Close()

	_, err = tarFile.Write([]byte("This is a test file inside the tar.\n"))
	require.NoError(t, err, "failed to write to tar file")
}

func TestCompress(t *testing.T) {
	tempDir := t.TempDir()
	tarFilePath := filepath.Join(tempDir, "test.tar")
	gzipFilePath := filepath.Join(tempDir, "test.tar.gz")

	t.Run("successful compression", func(t *testing.T) {
		createSampleTar(t, tarFilePath)

		err := Compress(tarFilePath, gzipFilePath)
		require.NoError(t, err, "expected no error during compression")

		_, err = os.Stat(gzipFilePath)
		require.NoError(t, err, "expected gzip file to be created, but it was not")
	})

	t.Run("file not found", func(t *testing.T) {
		err := Compress("non_existent.tar", gzipFilePath)
		require.Error(t, err, "expected error for non-existent tar file, got none")
	})

	t.Run("create gzip error", func(t *testing.T) {
		createSampleTar(t, tarFilePath)

		err := Compress(tarFilePath, "/invalid/path/to/gzip.tar.gz")
		require.Error(t, err, "expected error for invalid gzip file path, got none")
	})
}
