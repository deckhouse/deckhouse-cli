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

package loki

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"

	"github.com/spf13/cobra"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var lokiLong = templates.LongDesc(`
Dump Loki logs.
		
This command dump all logs from Loki api or in given range timestamps in DKP.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	lokiCmd := &cobra.Command{
		Use:           "loki",
		Short:         "Dump logs from Loki api.",
		Long:          lokiLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       flags.ValidateParameters,
		RunE:          backupLoki,
	}
	addFlags(lokiCmd.Flags())
	return lokiCmd
}

const (
	lokiURL            = "https://loki.d8-monitoring:3100/loki/api/v1"
	namespaceDeckhouse = "d8-system"
	containerName      = "deckhouse"
	namespaceLoki      = "d8-monitoring"
	secretNameLoki     = "loki-api-token"
	templateDate       = time.DateTime
)

var (
	endTimestamp   string
	startTimestamp string
	limitFlag      string
	chunkDaysFlag  int
	Logger         = log.NewSLogger(slog.LevelError)
)

type QueryRange struct {
	Data struct {
		Result []struct {
			Values [][]string `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

type SeriesApi struct {
	Data []map[string]string `json:"data"`
}

type CurlRequest struct {
	BaseURL   string
	Params    map[string]string
	AuthToken string
}

func backupLoki(cmd *cobra.Command, _ []string) error {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	token, err := getTokenLokiSa(kubeCl)
	if err != nil {
		return fmt.Errorf("error get token from secret for loki api: %w", err)
	}

	fmt.Println("Getting logs from Loki api...")

	endDumpTimestamp, err := getEndTimestamp(config, kubeCl, token)
	if err != nil {
		return fmt.Errorf("error get end timestamp for loki: %w", err)
	}
	chunkSize := time.Duration(chunkDaysFlag) * 24 * time.Hour
	for chunkEnd := endDumpTimestamp; chunkEnd > 0; chunkEnd -= chunkSize.Nanoseconds() {
		chunkStart := chunkEnd - chunkSize.Nanoseconds()
		if startTimestamp != "" {
			chunkStart, err = getStartTimestamp()
		}
		curlParamStreamList := CurlRequest{
			BaseURL: "series",
			Params: map[string]string{
				"end":   strconv.FormatInt(chunkEnd, 10),
				"start": strconv.FormatInt(chunkStart, 10),
			},
			AuthToken: token,
		}

		streamListDumpCurl := curlParamStreamList.GenerateCurlCommand()
		_, streamListDumpJson, err := getLogWithRetry(config, kubeCl, streamListDumpCurl)
		if err != nil {
			return fmt.Errorf("error get stream list JSON from loki: %w", err)
		}

		if len(streamListDumpJson.Data) == 0 {
			fmt.Printf("No more streams.\nStop...")
			break
		}

		for _, r := range streamListDumpJson.Data {
			err := fetchLogs(chunkStart, chunkEnd, endDumpTimestamp, token, r, config, kubeCl)
			if err != nil {
				return fmt.Errorf("error get logs from loki: %w", err)
			}
		}
	}
	return nil
}

func fetchLogs(chunkStart, chunkEnd, endDumpTimestamp int64, token string, r map[string]string, config *rest.Config, kubeCl kubernetes.Interface) error {
	var filters []string
	for key, value := range r {
		filters = append(filters, fmt.Sprintf(`%s=%q`, key, value))
	}
	q := fmt.Sprintf(`{%s}`, strings.Join(filters, ", "))

	chunkEnd = endDumpTimestamp
	for chunkEnd > chunkStart {
		curlParamDumpLog := CurlRequest{
			BaseURL: "query_range",
			Params: map[string]string{
				"end":       strconv.FormatInt(chunkEnd, 10),
				"start":     strconv.FormatInt(chunkStart, 10),
				"query":     q,
				"limit":     limitFlag,
				"direction": "BACKWARD",
			},
			AuthToken: token,
		}
		dumpLogCurl := curlParamDumpLog.GenerateCurlCommand()
		dumpLogCurlJson, _, err := getLogWithRetry(config, kubeCl, dumpLogCurl)
		if err != nil {
			return fmt.Errorf("error get JSON from Loki: %w", err)
		}

		if len(dumpLogCurlJson.Data.Result) == 0 {
			break
		}

		for _, d := range dumpLogCurlJson.Data.Result {
			for _, entry := range d.Values {
				timestampInt64, err := strconv.ParseInt(entry[0], 10, 64)
				if err != nil {
					return fmt.Errorf("error converting timestamp: %w", err)
				}
				timestampUtc := time.Unix(0, timestampInt64).UTC()
				fmt.Printf("Timestamp: [%v], Log: %s\n", timestampUtc, entry[1])
			}
		}
		// get last timestamp value from stream Loki api response to use pagination and get all log strings.
		lastLog := dumpLogCurlJson.Data.Result[0].Values[len(dumpLogCurlJson.Data.Result[0].Values)-1][0]
		lastTimestamp, err := strconv.ParseInt(lastLog, 10, 64)
		if err != nil {
			return fmt.Errorf("error converting timestamp: %w", err)
		}
		chunkEnd = lastTimestamp
	}
	return nil
}

func (c *CurlRequest) GenerateCurlCommand() []string {
	curlParts := append([]string{"curl", "--insecure", "-v"})
	curlParts = append(curlParts, fmt.Sprintf("%s/%s", lokiURL, c.BaseURL))
	for key, value := range c.Params {
		if value != "" {
			curlParts = append(curlParts, []string{"--data-urlencode", fmt.Sprintf("%s=%s", key, value)}...)
		}
	}
	if c.AuthToken != "" {
		curlParts = append(curlParts, []string{"-H", fmt.Sprintf("Authorization: Bearer %s", c.AuthToken)}...)
	}
	return curlParts
}

func getLogTimestamp(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesApi, error) {
	for _, apiUrlLoki := range fullCommand {
		var stdout, stderr bytes.Buffer

		podName, err := utilk8s.GetDeckhousePod(kubeCl)
		if err != nil {
			return nil, nil, err
		}
		executor, err := utilk8s.ExecInPod(config, kubeCl, fullCommand, podName, namespaceDeckhouse, containerName)
		if err != nil {
			return nil, nil, err
		}
		if err = executor.StreamWithContext(
			context.Background(),
			remotecommand.StreamOptions{
				Stdout: &stdout,
				Stderr: &stderr,
			}); err != nil {
			fmt.Fprint(os.Stderr, strings.Join(fullCommand, " "))
			return nil, nil, err
		}

		if apiUrlLoki == fmt.Sprintf("%s/series", lokiURL) {
			var series SeriesApi
			if !json.Valid(stdout.Bytes()) {
				return nil, nil, fmt.Errorf("error response from loki api: %s", stdout.String())
			}
			err = json.Unmarshal(stdout.Bytes(), &series)
			if err != nil {
				return nil, nil, fmt.Errorf("failed unmarshal loki response: %w", err)
			}
			return nil, &series, nil
		} else if apiUrlLoki == fmt.Sprintf("%s/query_range", lokiURL) {
			var queryRange QueryRange
			if !json.Valid(stdout.Bytes()) {
				return nil, nil, fmt.Errorf("error response from loki api: %s", stdout.String())
			}
			err = json.Unmarshal(stdout.Bytes(), &queryRange)
			if err != nil {
				return nil, nil, fmt.Errorf("failed unmarshal loki response: %w", err)
			}
			return &queryRange, nil, nil
		}
		stdout.Reset()
	}

	return nil, nil, nil
}

func getEndTimestamp(config *rest.Config, kubeCl kubernetes.Interface, token string) (int64, error) {
	if endTimestamp == "" {
		endTimestampCurlParam := CurlRequest{
			BaseURL: "query_range",
			Params: map[string]string{
				"query":     `{pod=~".+"}`,
				"limit":     "1",
				"direction": "BACKWARD",
			},
			AuthToken: token,
		}
		endTimestampCurl := endTimestampCurlParam.GenerateCurlCommand()
		endTimestampJson, _, err := getLogWithRetry(config, kubeCl, endTimestampCurl)
		if err != nil {
			return 0, fmt.Errorf("error get latest timestamp JSON from loki: %w", err)
		}
		endTimestamp, err := strconv.ParseInt(endTimestampJson.Data.Result[0].Values[0][0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("error converting timestamp: %w", err)
		}
		return endTimestamp, err
	}

	end, err := time.Parse(templateDate, endTimestamp)
	if err != nil {
		return 0, fmt.Errorf("error parsing date: %w, please provide correct date", err)
	}
	endTimestampNanoSec := end.UnixNano()

	return endTimestampNanoSec, err
}

func getStartTimestamp() (int64, error) {
	start, err := time.Parse(templateDate, startTimestamp)
	if err != nil {
		return 0, fmt.Errorf("error parsing date: %w, please provide correct date", err)
	}
	startTimestampNanoSec := start.UnixNano()

	return startTimestampNanoSec, nil
}

func getTokenLokiSa(kubeCl kubernetes.Interface) (string, error) {
	secret, err := kubeCl.CoreV1().Secrets(namespaceLoki).Get(context.TODO(), secretNameLoki, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get secret: %w", err)
	}

	tokenBase64, exists := secret.Data["token"]
	if !exists {
		return "", fmt.Errorf("token not found in secret: %w", err)
	}
	return string(tokenBase64), err
}

func getLogWithRetry(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesApi, error) {

	var (
		err            error
		QueryRangeDump *QueryRange
		SeriesApiDump  *SeriesApi
	)

	err = retry.RunTask(Logger,
		"error get json response from Loki",
		task.WithConstantRetries(5, 10*time.Second, func(ctx context.Context) error {
			QueryRangeDump, SeriesApiDump, err = getLogTimestamp(config, kubeCl, fullCommand)
			if err != nil {
				return fmt.Errorf("error get JSON response from loki: %w", err)
			}
			return nil
		}))
	if err != nil {
		return nil, nil, fmt.Errorf("error get JSON from loki: %w", err)
	}
	return QueryRangeDump, SeriesApiDump, nil
}
