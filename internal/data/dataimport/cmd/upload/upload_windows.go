//go:build windows

package upload

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	client "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
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

	cmd.Flags().StringP("namespace", "n", dataio.Namespace, "data volume namespace")
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
			// On Windows, UID/GID are not applicable, keep defaults (-1)
		}
	}

	// Resolve the producer that serves DataImport here, and build the runtime client bound to
	// it for publish auto-detection and reconciliation.
	backend, rtClient, err := util.ResolveClientFunc(ctx, httpClient, namespace, log)
	if err != nil {
		return err
	}

	publishFlag, err := dataio.ParsePublishFlag(cmd.Flags())
	if err != nil {
		return err
	}

	publish, err := dataio.ResolvePublish(ctx, publishFlag, rtClient, httpClient, log)
	if err != nil {
		return err
	}

	podUrl, baseUrl, _, subClient, err := util.PrepareUploadFunc(ctx, backend, diName, namespace, publish, httpClient, log)
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

	if err := upload(ctx, log, subClient, fileUrl, pathToFile, chunks, permOctal, uid, gid, resume); err != nil {
		return err
	}

	// Finalise the upload: streaming the bytes is not enough. The importer keeps the upload
	// session open until it receives POST /api/v1/finished, and only then does the controller
	// set UploadFinished=True and rebind the target / mark the DataImport Completed.
	return util.PostFinished(ctx, subClient, baseUrl)
}
