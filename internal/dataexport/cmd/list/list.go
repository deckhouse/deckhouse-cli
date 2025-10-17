/*
Copyright 2024 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package list

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/deckhouse-cli/internal/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/dataexport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "list"
)

func cmdExamples() string {
	resp := []string{
		fmt.Sprintf(`  ... -n target-namespace %s my-file-volume /mydir/testdir/`, cmdName),
		fmt.Sprintf(`  ... -n target-namespace %s my-block-volume`, cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_export_name [/path/]",
		Aliases: []string{"ls"},
		Short:   "List DataExported content information",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(_ *cobra.Command, args []string) error {
			_, _, err := parseArgs(args)
			return err
		},
	}

	cmd.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")
	cmd.Flags().String("ttl", "2m", "Time to live for auto-created DataExport")

	return cmd
}

func parseArgs(args []string) (deName, srcPath string, err error) {
	if len(args) < 1 || len(args) > 2 {
		err = fmt.Errorf("invalid arguments")
		return
	}

	deName, srcPath = args[0], ""
	if len(args) >= 2 {
		srcPath = args[1]
	}

	return
}

func downloadFunc(
	ctx context.Context,
	log *slog.Logger,
	namespace, deName, srcPath string,
	publish bool,
	sClient *safeClient.SafeClient,
	foo func(body io.Reader) error,
) error {
	url, volumeMode, subClient, err := util.PrepareDownloadFunc(ctx, log, deName, namespace, publish, sClient)
	if err != nil {
		return err
	}

	var req *http.Request
	switch volumeMode {
	case "Filesystem":
		if srcPath == "" || srcPath[len(srcPath)-1:] != "/" {
			return fmt.Errorf("invalid source path: '%s'", srcPath)
		}
		dataURL, err := neturl.JoinPath(url, srcPath)
		if err != nil {
			return err
		}

		log.Info("Start listing", slog.String("url", dataURL), slog.String("srcPath", srcPath))
		req, _ = http.NewRequest(http.MethodGet, dataURL, nil)
	case "Block":
		log.Info("Start listing", slog.String("url", url))
		req, _ = http.NewRequest(http.MethodHead, url, nil)
	default:
		return fmt.Errorf("%w: %s", util.ErrUnsupportedVolumeMode, volumeMode)
	}

	resp, err := subClient.HTTPDo(req.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("http do: %s", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		const maxLen = 4096
		msg, err := io.ReadAll(io.LimitReader(resp.Body, maxLen))
		if err != nil {
			return fmt.Errorf("Backend response \"%s\"", resp.Status)
		}
		return fmt.Errorf("Backend response \"%s\" Msg: %s", resp.Status, string(msg))
	}

	switch volumeMode {
	case "Block":
		body := ""
		if contLen := resp.Header.Get("Content-Length"); contLen != "" {
			// Convert raw bytes value to human-readable size using k8s quantity library.
			// We deliberately ignore conversion errors and fallback to raw bytes if any.
			if size, err := strconv.ParseInt(contLen, 10, 64); err == nil {
				q := resource.NewQuantity(size, resource.BinarySI)
				body = fmt.Sprintf("Disk size: %s", q.String())
			} else {
				body = fmt.Sprintf("Disk size: %s bytes", contLen)
			}
			// Ensure the size information is printed on a dedicated line for better readability.
			body += "\n"
		}
		return foo(strings.NewReader(body))
	case "Filesystem":
		return foo(resp.Body)
	default:
		return fmt.Errorf("%w: %s", util.ErrUnsupportedVolumeMode, volumeMode)
	}
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	namespace, _ := cmd.Flags().GetString("namespace")
	publish, _ := cmd.Flags().GetBool("publish")
	ttl, _ := cmd.Flags().GetString("ttl")

	dataName, srcPath, err := parseArgs(args)
	if err != nil {
		return fmt.Errorf("arguments parsing error: %s", err.Error())
	}

	flags := cmd.PersistentFlags()
	safeClient.SupportNoAuth = false
	sClient, err := safeClient.NewSafeClient(flags)
	if err != nil {
		return err
	}

	rtClient, err := sClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return err
	}
	deName, err := util.CreateDataExporterIfNeededFunc(ctx, log, dataName, namespace, publish, ttl, rtClient)
	if err != nil {
		return err
	}

	log.Info("DataExport created", slog.String("name", deName), slog.String("namespace", namespace))

	err = downloadFunc(ctx, log, namespace, deName, srcPath, publish, sClient, func(body io.Reader) error {
		_, err := io.Copy(os.Stdout, body)
		if err == io.EOF {
			err = nil
		}
		return err
	})

	if err != nil {
		return err
	}

	if deName != dataName { // DataExport created in download process
		if util.AskYesNoWithTimeout("DataExport will auto-delete in 30 sec [press y+Enter to delete now, n+Enter to cancel]", time.Second*30) {
			util.DeleteDataExport(ctx, deName, namespace, rtClient)
		}
	}

	return nil
}
