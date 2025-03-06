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
	"github.com/deckhouse/deckhouse-cli/internal/platform/flags"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"os"
	"strconv"
	"strings"
	"time"

	//"github.com/deckhouse/deckhouse-cli/internal/platform/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/spf13/cobra"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/kubectl/pkg/util/templates"
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
	lokiURL            = "https://loki.d8-monitoring.svc.cluster.local:3100/loki/api/v1"
	labelSelector      = "leader=true"
	namespaceDeckhouse = "d8-system"
	containerName      = "deckhouse"
	namespaceLoki      = "d8-monitoring"
	secretNameLoki     = "loki-api-token"
	templateDate       = "2006-01-02 15:04:05"

	chunkSize = int64(30 * 24 * 60 * 60 * 1e9) //30 days in nanosec timestamp
)

var (
	endTimestamp   string
	startTimestamp string
	limitFlag      string
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
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	token, err := getTokenLokiSa(kubeCl)
	if err != nil {
		return fmt.Errorf("Error get token from secret for Loki api: %s", err)
	}

	endDumpTimestamp, err := getEndTimestamp(config, kubeCl, token)
	if err != nil {
		return fmt.Errorf("Error get end timestamp for Loki: %s", err)
	}
	for chunkEnd := endDumpTimestamp; chunkEnd > 0; chunkEnd -= chunkSize {
		chunkStart := chunkEnd - chunkSize
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
		_, streamListDumpJson, err := getLogTimestamp(config, kubeCl, streamListDumpCurl)
		if err != nil {
			return fmt.Errorf("Error get stream list JSON from Loki: %s", err)
		}
		if len(streamListDumpJson.Data) == 0 {
			fmt.Printf("No more streams.\nStop...")
			break
		}
		for _, r := range streamListDumpJson.Data {
			err := fetchLogs(chunkStart, chunkEnd, endDumpTimestamp, token, r, config, kubeCl)
			if err != nil {
				return fmt.Errorf("Error get logs from Loki: %s", err)
			}
		}
	}
	return err
}

func fetchLogs(chunkStart, chunkEnd, endDumpTimestamp int64, token string, r map[string]string, config *rest.Config, kubeCl kubernetes.Interface) error {
	var filters []string
	for key, value := range r {
		filters = append(filters, fmt.Sprintf(`%s="%s"`, key, value))
	}
	q := fmt.Sprintf(`{%s}`, strings.Join(filters, ", "))

	chunkEnd = endDumpTimestamp
	for chunkEnd > chunkStart {
		limit := "5000"
		if limitFlag != "" {
			limit = limitFlag
		}

		curlParamDumpLog := CurlRequest{
			BaseURL: "query_range",
			Params: map[string]string{
				"end":       strconv.FormatInt(chunkEnd, 10),
				"start":     strconv.FormatInt(chunkStart, 10),
				"query":     q,
				"limit":     limit,
				"direction": "BACKWARD",
			},
			AuthToken: token,
		}
		DumpLogCurl := curlParamDumpLog.GenerateCurlCommand()
		DumpLogCurlJson, _, err := getLogTimestamp(config, kubeCl, DumpLogCurl)
		if err != nil {
			//errChan <- fmt.Errorf("Error get JSON from Loki: %s", err)
			return fmt.Errorf("Error get JSON from Loki: %s", err)
		}
		if len(DumpLogCurlJson.Data.Result) == 0 {
			break
		}

		for _, d := range DumpLogCurlJson.Data.Result {
			for _, entry := range d.Values {
				timestampInt64, err := strconv.ParseInt(entry[0], 10, 64)
				if err != nil {
					return fmt.Errorf("Error converting timestamp: %s", err)
				}
				timestampUtc := time.Unix(0, timestampInt64).UTC()
				fmt.Printf("Timestamp: [%v], Log: %s\n", timestampUtc, entry[1])
			}
		}
		firstLog := DumpLogCurlJson.Data.Result[len(DumpLogCurlJson.Data.Result)-1].Values[len(DumpLogCurlJson.Data.Result[len(DumpLogCurlJson.Data.Result)-1].Values)-1][0]
		firstTimestamp, err := strconv.ParseInt(firstLog, 10, 64)
		if err != nil {
			return fmt.Errorf("Error converting timestamp: %s", err)
		}
		chunkEnd = firstTimestamp

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

func GetDeckhousePod(kubeCl kubernetes.Interface, namespace string, labelSelector string) (string, error) {
	pods, err := kubeCl.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return "", fmt.Errorf("Error listing pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("No pods found with the label: %s", labelSelector)
	}

	pod := pods.Items[0]
	podName := pod.Name
	return podName, nil
}

func ExecInPod(config *rest.Config, kubeCl kubernetes.Interface, command []string, podName string, namespace string, containerName string) (remotecommand.Executor, error) {
	scheme := runtime.NewScheme()
	parameterCodec := runtime.NewParameterCodec(scheme)
	if err := v1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("Failed to create parameter codec: %w", err)
	}

	req := kubeCl.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Command:   command,
			Container: containerName,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, parameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("Creating SPDY executor for Pod %s: %v", podName, err)
	}
	return executor, nil
}

func getLogTimestamp(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesApi, error) {
	for _, t := range fullCommand {
		var stdout, stderr bytes.Buffer
		podName, err := GetDeckhousePod(kubeCl, namespaceDeckhouse, labelSelector)
		executor, err := ExecInPod(config, kubeCl, fullCommand, podName, namespaceDeckhouse, containerName)
		if err = executor.StreamWithContext(
			context.Background(),
			remotecommand.StreamOptions{
				Stdout: &stdout,
				Stderr: &stderr,
			}); err != nil {
			fmt.Fprintf(os.Stderr, strings.Join(fullCommand, " "))
			return nil, nil, err
		}

		if t == fmt.Sprintf("%s/series", lokiURL) {
			var series SeriesApi
			if !json.Valid(stdout.Bytes()) {
				return nil, nil, fmt.Errorf("Error response from loki api: %s", stdout.String())
			}
			err = json.Unmarshal(stdout.Bytes(), &series)
			if err != nil {
				return nil, nil, fmt.Errorf("Failed unmarshal SeriesApi: %s", err)
			}
			return nil, &series, nil
		} else if t == fmt.Sprintf("%s/query_range", lokiURL) {
			var queryRange QueryRange
			if !json.Valid(stdout.Bytes()) {
				return nil, nil, fmt.Errorf("Error response from loki api: %s", stdout.String())
			}
			err = json.Unmarshal(stdout.Bytes(), &queryRange)
			if err != nil {
				return nil, nil, fmt.Errorf("Failed unmarshal LokiResponse: %s", err)
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
		endTimestampJson, _, err := getLogTimestamp(config, kubeCl, endTimestampCurl)
		if err != nil {
			return 0, fmt.Errorf("Error get latest timestamp JSON from Loki: %s", err)
		}
		endTimestamp, err := strconv.ParseInt(endTimestampJson.Data.Result[0].Values[0][0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("Error converting timestamp: %s", err)
		}
		return endTimestamp, nil
	}

	end, err := time.Parse(templateDate, endTimestamp)
	if err != nil {
		return 0, fmt.Errorf("Error parsing date: %s, please provide correct date.", err)
	}
	endTimestampNanoSec := end.UnixNano()

	return endTimestampNanoSec, nil
}

func getStartTimestamp() (int64, error) {
	start, err := time.Parse(templateDate, startTimestamp)
	if err != nil {
		return 0, fmt.Errorf("Error parsing date: %s, please provide correct date.", err)
	}
	startTimestampNanoSec := start.UnixNano()

	return startTimestampNanoSec, nil
}

func getTokenLokiSa(kubeCl kubernetes.Interface) (string, error) {
	secret, err := kubeCl.CoreV1().Secrets(namespaceLoki).Get(context.TODO(), secretNameLoki, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("Failed to get Secret: %v", err)
	}

	tokenBase64, exists := secret.Data["token"]
	if !exists {
		return "", fmt.Errorf("Token not found in Secret: %v", err)
	}
	return string(tokenBase64), err
}
