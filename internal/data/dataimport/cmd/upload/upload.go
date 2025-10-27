package upload

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	"github.com/deckhouse/deckhouse-cli/internal/dataio"
	client "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
	"github.com/spf13/cobra"
)

const (
	cmdName                = "upload"
	defaultFilePermissions = "0644"
)

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_import_name path/file.ext",
		Short:   "Upload a file to the provided url",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid arguments")
			}
			return nil
		},
	}

	cmd.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")
	cmd.Flags().StringP("file", "f", "", "file to upload")
	cmd.Flags().IntP("chunks", "c", 10, "number of chunks to upload")
	cmd.Flags().BoolP("publish", "P", false, "publish the uploaded file")
	cmd.Flags().StringP("dstPath", "d", "", "destination path of the uploaded file")
	cmd.Flags().Bool("resume", false, "resume upload if process was interrupted")

	return cmd
}

func cmdExamples() string {
	resp := []string{
		"  # Upload with resume (continue from server-reported offset)",
		fmt.Sprintf("    ... %s NAME -n NAMESPACE -P -d /dst/path -f ./file --resume", cmdName),
		"  # Upload without resume, split into 4 chunks",
		fmt.Sprintf("    ... %s NAME -n NAMESPACE -P -d /dst/path -f ./file -c 4", cmdName),
	}
	return strings.Join(resp, "\n")
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	pathToFile, _ := cmd.Flags().GetString("file")
	chunks, _ := cmd.Flags().GetInt("chunks")
	publish, _ := cmd.Flags().GetBool("publish")
	namespace, _ := cmd.Flags().GetString("namespace")
	dstPath, _ := cmd.Flags().GetString("dstPath")
	resume, _ := cmd.Flags().GetBool("resume")

	flags := cmd.PersistentFlags()
	httpClient, err := client.NewSafeClient(flags)
	if err != nil {
		return err
	}

	diName, _, err := dataio.ParseArgs(args)
	if err != nil {
		return err
	}

	log.Info("Run")

	permOctal := defaultFilePermissions
	uid := os.Getuid()
	gid := os.Getgid()
	if pathToFile != "" && pathToFile != "-" {
		if fi, statErr := os.Stat(pathToFile); statErr == nil {
			permOctal = fmt.Sprintf("%04o", fi.Mode().Perm())
			if st, ok := fi.Sys().(*syscall.Stat_t); ok {
				uid = int(st.Uid)
				gid = int(st.Gid)
			}
		}
	}

	podUrl, _, subClient, err := util.PrepareUpload(ctx, log, diName, namespace, publish, httpClient)
	if err != nil {
		return err
	}

	fileUrl, err := url.JoinPath(podUrl, dstPath)
	if err != nil {
		return err
	}

	if chunks < 1 {
		chunks = 1
	}

	return upload(ctx, log, subClient, fileUrl, pathToFile, chunks, permOctal, uid, gid, resume)
}

func upload(ctx context.Context, log *slog.Logger, httpClient *client.SafeClient, url string, filePath string, chunks int, permOctal string, uid, gid int, resume bool) error {
	var offset int64 = 0
	if resume {
		off, err := util.CheckUploadProgress(ctx, httpClient, url)
		if err != nil {
			return err
		}
		offset = off
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return err
	}

	totalSize := fi.Size()
	if totalSize < 0 {
		return fmt.Errorf("invalid file size")
	}

	chunkSize := totalSize / int64(chunks)
	if totalSize%int64(chunks) != 0 {
		chunkSize++
	}

	for offset < totalSize {
		remaining := totalSize - offset
		sendLen := chunkSize
		if sendLen > remaining {
			sendLen = remaining
		}

		section := io.NewSectionReader(file, offset, sendLen)
		req, err := http.NewRequest(http.MethodPut, url, io.NopCloser(section))
		if err != nil {
			return err
		}
		req = req.WithContext(ctx)

		req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
		req.Header.Set("X-Attribute-Permissions", permOctal)
		req.Header.Set("X-Attribute-Uid", strconv.Itoa(uid))
		req.Header.Set("X-Attribute-Gid", strconv.Itoa(gid))
		req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))

		resp, err := httpClient.HTTPDo(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("server error at offset %d: status %d (%s)", offset, resp.StatusCode, resp.Status)
		}

		nextOffsetStr := resp.Header.Get("X-Next-Offset")
		if nextOffsetStr == "" {
			offset += sendLen
			continue
		}
		nextOffset, err := strconv.ParseInt(nextOffsetStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid X-Next-Offset: %s: %w", nextOffsetStr, err)
		}
		if nextOffset < offset {
			return fmt.Errorf("server returned X-Next-Offset (%d) smaller than current offset (%d)", nextOffset, offset)
		}
		offset = nextOffset
	}

	return nil
}
