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

package download

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/dataexport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "download"
)

func cmdExamples() string {
	resp := []string{
		fmt.Sprintf("  # Start exporter + Download + Stop for Filesystem"),
		fmt.Sprintf("    ... %s [flags] kind/volume_name path/file.ext [-o out_file.ext]", cmdName),
		fmt.Sprintf("    ... %s -n target-namespace PVC/my-file-volume mydir/testdir/file.txt -o file.txt", cmdName),
		fmt.Sprintf("  # Start exporter + Download + Stop for Block"),
		fmt.Sprintf("    ... %s [flags] kind/volume_name [-o out_file.ext]", cmdName),
		fmt.Sprintf("    ... %s -n target-namespace VS/my-vs-volume -o file.txt", cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] [KIND/]data_export_name [path/file.ext]",
		Short:   "Download exported data",
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
	cmd.Flags().StringP("output", "o", "", "file to save data (default: same as resource)") //TODO support /dev/stdout
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")

	return cmd
}

func parseArgs(args []string) (deName, srcPath string, err error) {
	switch len(args) {
	case 1:
		deName = args[0]
	case 2:
		deName = args[0]
		srcPath = args[1]
	default:
		return "", "", fmt.Errorf("invalid arguments")
	}

	if !strings.HasPrefix(srcPath, "/") {
		srcPath = "/" + srcPath
	}

	return
}

type dirItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type dirResp struct {
	Version string    `json:"apiVersion"`
	Items   []dirItem `json:"items"`
}

func forRespItems(jsonStream io.ReadCloser, workFunc func(*dirItem) error) error {
	dec := json.NewDecoder(jsonStream)

	// find items list
	for {
		t, err := dec.Token()
		if err != nil {
			return err
		}

		if t == "items" {
			t, err := dec.Token()
			if err != nil {
				return err
			}
			if t != json.Delim('[') {
				return fmt.Errorf("JSON items is not list")
			}
			break
		}
		dec.More()
	}

	// read items
	for dec.More() {
		var i dirItem
		err := dec.Decode(&i)
		if err != nil {
			break
		}
		err = workFunc(&i)
		if err != nil {
			return err
		}
	}

	// check items list closed
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if t != json.Delim(']') {
		return fmt.Errorf("items loading is not completed")
	}

	return nil
}

func recursiveDownload(ctx context.Context, sClient *safeClient.SafeClient, log *slog.Logger, sem chan struct{}, url, srcPath, dstPath string) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	dataURL, err := neturl.JoinPath(url, srcPath)
	if err != nil {
		return err
	}

	req, _ := http.NewRequest("GET", dataURL, nil)
	resp, err := sClient.HttpDo(req)
	if err != nil {
		return fmt.Errorf("HttpDo: %s\n", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		if resp.ContentLength > 0 {
			msg, err := io.ReadAll(io.LimitReader(resp.Body, 1000))
			if err == nil {
				return fmt.Errorf("Backend response \"%s\" Msg: %s", resp.Status, string(msg))
			}
		}

		return fmt.Errorf("Backend response \"%s\"", resp.Status)
	}

	if srcPath != "" && srcPath[len(srcPath)-1:] == "/" {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error

		err = forRespItems(resp.Body, func(item *dirItem) error {
			subPath := item.Name
			if item.Type == "dir" {
				err = os.MkdirAll(filepath.Join(dstPath, subPath), os.ModePerm)
				if err != nil {
					return fmt.Errorf("Create dir error: %s", err.Error())
				}
				subPath += "/"
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(sp string) {
				defer func() { <-sem; wg.Done() }()
				subErr := recursiveDownload(ctx, sClient, log, sem, url, srcPath+sp, filepath.Join(dstPath, sp))
				if subErr != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("Download %s: %w", filepath.Join(srcPath, sp), subErr)
					}
					mu.Unlock()
				}
			}(subPath)

			return nil
		})
		if err != nil {
			return fmt.Errorf("Response body (%s) error: %s", srcPath, err.Error())
		}

		wg.Wait()
		return firstErr
	} else {
		if dstPath != "" {
			// Create out file
			out, err := os.Create(dstPath)
			if err != nil {
				return err
			}
			defer out.Close()

			_, err = io.Copy(out, resp.Body)
			if err != nil {
				return err
			}
			log.Info("Downloaded file", slog.String("path", dstPath))
		} else {
			_, err = io.Copy(os.Stdout, resp.Body)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	namespace, _ := cmd.Flags().GetString("namespace")
	dstPath, _ := cmd.Flags().GetString("output")
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

	deName, err := util.CreateDataExporterIfNeeded(ctx, log, dataName, namespace, publish, rtClient)
	if err != nil {
		return err
	}

	log.Info("DataExport created", slog.String("name", deName), slog.String("namespace", namespace))

	url, volumeMode, subClient, err := util.PrepareDownload(ctx, log, deName, namespace, publish, sClient)
	if err != nil {
		return err
	}

	switch volumeMode {
	case "Filesystem":
		if srcPath == "" {
			return fmt.Errorf("invalid source path: '%s'", srcPath)
		}
		if dstPath == "" {
			pathList := strings.Split(srcPath, "/")
			dstPath = pathList[len(pathList)-1]
		}
	case "Block":
		srcPath = ""
		if dstPath == "" {
			dstPath = deName
		}
	default:
		return fmt.Errorf("%w: %s", util.UnsupportedVolumeModeErr, volumeMode)
	}

	log.Info("Start downloading", slog.String("url", url+srcPath), slog.String("dstPath", dstPath))
	sem := make(chan struct{}, 10)
	err = recursiveDownload(ctx, subClient, log, sem, url, srcPath, dstPath)
	if err != nil {
		log.Error("Not all files have been downloaded", slog.String("error", err.Error()))
	} else {
		log.Info("All files have been downloaded", slog.String("dstPath", dstPath))
	}

	if deName != dataName { // DataExport created in download process
		if util.AskYesNoWithTimeout("DataExport will auto-delete in 30 sec [press y+Enter to delete now, n+Enter to cancel]", time.Second*30) {
			util.DeleteDataExport(ctx, deName, namespace, rtClient)
		}
	}

	return nil
}
