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
	"strings"
	"time"

	"github.com/spf13/cobra"

	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

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
		Args: func(cmd *cobra.Command, args []string) error {
			_, _, err := parseArgs(args)
			return err
		},
	}

	cmd.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")

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

func downloadRaw(ctx context.Context, log *slog.Logger, namespace, deName, srcPath string, publish bool, sClient *safeClient.SafeClient) ([]byte, error) {
	url, volumeMode, subClient, err := util.PrepareDownload(ctx, log, deName, namespace, publish, sClient)
	if err != nil {
		return nil, err
	}

	var req *http.Request
	if volumeMode == "Filesystem" {
		if srcPath == "" || srcPath[len(srcPath)-1:] != "/" {
			return nil, fmt.Errorf("invalid source path: '%s'", srcPath)
		}
		dataURL, err := neturl.JoinPath(url, srcPath)
		if err != nil {
			return nil, err
		}

		log.Info("Start listing", slog.String("url", dataURL), slog.String("srcPath", srcPath))
		req, _ = http.NewRequest("GET", dataURL, nil)
	} else if volumeMode == "Block" {
		log.Info("Start listing", slog.String("url", url))
		req, _ = http.NewRequest("HEAD", url, nil)
	}

	resp, err := subClient.HttpDo(req)
	if err != nil {
		return nil, fmt.Errorf("HttpDo: %s\n", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		if resp.ContentLength > 0 {
			msg, err := io.ReadAll(io.LimitReader(resp.Body, 1000))
			if err == nil {
				return nil, fmt.Errorf("Backend response \"%s\" Msg: %s", resp.Status, string(msg))
			}
		}

		return nil, fmt.Errorf("Backend response \"%s\"", resp.Status)
	}

	bodyRaw := []byte{}
	if volumeMode == "Block" {
		contLen := resp.Header.Get("Content-Length")
		if len(contLen) == 0 {
			contLen = "0"
		}
		bodyRaw = append(bodyRaw, []byte("Content-Length: "+contLen+"\n")...)
	} else {
		bodyRaw, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("Response body (%s) error: %s", srcPath, err.Error())
		}
	}

	return bodyRaw, nil
}

func createDataExporterIfNeeded(ctx context.Context, log *slog.Logger, deName, namespace string, publish bool, rtClient ctrlrtclient.Client) (string, error) {
	var volumeKind, volumeName string
	name := strings.ToLower(deName)
	if strings.HasPrefix(name, "pvc/") {
		volumeKind = "PersistentVolumeClaim"
		volumeName = deName[4:]
		deName = "de-pvc-" + volumeName
	} else if strings.HasPrefix(name, "vs/") {
		volumeKind = "VolumeSnapshot"
		volumeName = deName[3:]
		deName = "de-vs-" + volumeName
	} else {
		return deName, nil
	}

	err := util.CreateDataExport(ctx, deName, namespace, "", volumeKind, volumeName, publish, rtClient)
	if err != nil {
		return deName, err
	}
	log.Info("DataExport creating", slog.String("name", deName), slog.String("namespace", namespace))

	return deName, nil
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	namespace, _ := cmd.Flags().GetString("namespace")
	publish, _ := cmd.Flags().GetBool("publish")

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
	deName, err := createDataExporterIfNeeded(ctx, log, dataName, namespace, publish, rtClient)
	if err != nil {
		return err
	}

	log.Info("DataExport created", slog.String("name", deName), slog.String("namespace", namespace))

	data, err := downloadRaw(ctx, log, namespace, deName, srcPath, publish, sClient)
	if err != nil {
		return err
	}
	fmt.Println(string(data))

	if deName != dataName { // DataExport created in download process
		if util.AskYesNoWithTimeout("DataExport will auto-delete in 30 sec [press y+Enter to delete now, n+Enter to cancel]", time.Second*30) {
			util.DeleteDataExport(ctx, deName, namespace, rtClient)
		}
	}

	return nil
}
