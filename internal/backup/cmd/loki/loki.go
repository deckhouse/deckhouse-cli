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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"os"
	"strconv"
	"strings"
	//"github.com/deckhouse/deckhouse-cli/internal/platform/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/spf13/cobra"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/kubectl/pkg/util/templates"
)

var lokiLong = templates.LongDesc(`
Take a snapshot of ETCD state.
		
This command creates a snapshot of the Kubernetes underlying key-value database ETCD.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	lokiCmd := &cobra.Command{
		Use:           "loki <snapshot-path>",
		Short:         "Take a snapshot of ETCD state",
		Long:          lokiLong,
		ValidArgs:     []string{"snapshot-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		//PreRunE:       flags.ValidateParameters,
		RunE: backupLoki,
	}

	//addFlags(lokiCmd.Flags())
	return lokiCmd
}

const (
	//lokiURL = "https://loki.d8-monitoring.svc.cluster.local:3100/loki/api/v1/query_range"
	lokiURL = "https://loki.d8-monitoring.svc.cluster.local:3100/loki/api/v1"
	//lokiURL      = "https://loki.d8-monitoring.svc.cluster.local:3100/loki/api/v1/series"
	parallelJobs = 5             // Number of parallel requests
	query        = `{pod=~".+"}` // LogQL query
	//query = `query={pod=~".+"}` // LogQL query
	//startTime    = "2025-02-12T16:22:00Z" // Start time
	//endTime      = "2025-02-12T16:25:00Z" // End time
	limit              = `limit=10` // Number of logs per query
	direction          = `direction=FORWARD`
	labelSelector      = "leader=true"
	namespaceDeckhouse = "d8-system"
	containerName      = "deckhouse"
	chunkDays          = 30
	workers            = 5
)

// LokiResponse Struct to store API response query_range
type LokiResponse struct {
	Data struct {
		Result []struct {
			//Stream    map[string]string `json:"stream"`
			Values [][]string `json:"values"`
			//Values []struct {
			//	Timestamp int64 `json:"[0]"`
			//}
			Stream struct {
				Pod       string `json:"pod"`
				Container string `json:"container"`
			} `json:"stream"`
		} `json:"result"`
	} `json:"data"`
}

type SeriesApi struct {
	Data []struct {
		Pod       string `json:"pod"`
		Container string `json:"container"`
	} `json:"data"`
}

//type Command struct {
//	Cmd  string
//	Args []string
//	File string
//}

type CurlRequest struct {
	BaseURL string // Base URL of the request
	//Headers   map[string]string // Headers to include in the request
	Params    map[string]string // Query parameters (dynamic --data-urlencode)
	AuthToken string            // Bearer token (optional)
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

	//podName, err := GetDeckhousePod(kubeCl, namespaceDeckhouse, labelSelector)
	//
	//var stdout, stderr bytes.Buffer

	//gzipWriter := gzip.NewWriter(os.Stdout)
	//defer gzipWriter.Close()
	//tarWriter := tar.NewWriter(gzipWriter)
	//defer tarWriter.Close()

	//for _, cmd := range debugCommands {
	//
	//	//fullCommand := append([]string{cmd.Cmd}, cmd.Args...)
	//
	//	executor, err := ExecInPod(config, kubeCl, fullCommand, podName, namespace, containerName)
	//	if err = executor.StreamWithContext(
	//		context.Background(),
	//		remotecommand.StreamOptions{
	//			Stdout: &stdout,
	//			Stderr: &stderr,
	//		}); err != nil {
	//		fmt.Fprintf(os.Stderr, strings.Join(fullCommand, " "))
	//		fmt.Fprintf(os.Stderr, stderr.String())
	//	}
	//
	//	//if err = cmd.Writer(tarWriter, stdout.Bytes())
	//	// err != nil {
	//	//	return fmt.Errorf("failed to update the %s", err)
	//	//}
	//	fmt.Fprintf(os.Stdout, stdout.String())
	//	stdout.Reset()
	//
	//

	//var startTS int64
	//// Extract timestamp from response
	//if len(result.Data.Result) > 0 && len(result.Data.Result[0].Values) > 0 {
	//	//return result.Data.Result[0].Values[0][0], nil // First log's timestamp
	//	//startTS = result.Data.Result[0].Values[0][0] // First log's timestamp
	//	startTS, err = strconv.ParseInt(result.Data.Result[0].Values[0][0], 10, 64) // First log's timestamp
	//	if err != nil {
	//		return fmt.Errorf("Error converting timestamp:", err)
	//	}
	//}

	// Convert time range to Unix timestamps
	//startTS, _ := time.Parse(time.RFC3339, startTime)
	//endTS := time.Now().UnixNano()
	//totalDuration := endTS - startTS
	//endTS, _ := time.Parse(time.RFC3339, endTime)
	//totalDuration := endTS.Sub(startTS)

	// Calculate chunk size (divide total duration by parallel jobs)
	//chunkSize := totalDuration / time.Duration(parallelJobs)
	//
	//chunkStart := startTS.Add(time.Duration(1) * chunkSize)
	//chunkEnd := chunkStart.Add(chunkSize)

	//fmt.Printf("Fetching logs from %s to %s\n", chunkStart, chunkEnd)

	token := "eyJhbGciOiJSUzI1NiIsImtpZCI6IkFnbVRCVndWRm43dy04Qmg1cENqcXFQMVFhOEhuLXF0dUpFSTdWQXBYYUkifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkOC1tb25pdG9yaW5nIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6Imxva2ktYXBpLXRva2VuIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6Imxva2kiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiI0N2Y2ZWY1Ni01YjdkLTRlNjUtYTc3Zi1mNTI0ODkyZDJhNzgiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6ZDgtbW9uaXRvcmluZzpsb2tpIn0.EF-RGqY0acC-C_2KPz51UPdwkLMGw-DV2nsrJuh2lQZ_0ebiwTmWoVFCj6o7Ey2z9CsNHkvEr9jxTc7uHh0rvRQIJp5rUrimeSBfvrJpLaiiVQ_h5cXJN84l5jq4IkbzO7lUObtjh6DmNzodZCbxMEu-Gm766weRhUdoW8zco7Cd-m26sQK4095tp9_4iW5lXBGC6R68DEa-2pjZjHpDspRwnI4XY_BVXldaIKpbR5cCU-8CKzJ0BXSvDcjKUjFv3Mk0TomMSFSlnMY5wyvr6vvus11E3MxajRq1vL9PJiW1ZfBFRnwEQsQnsIPgQMb45fmpgayCLMBnmjNF4WRxvg"

	//curlParamFirstTS := CurlRequest{
	//	Params: map[string]string{
	//		"query":     `{pod=~".+"}`,
	//		"limit":     "1",
	//		"direction": "FORWARD",
	//	},
	//	AuthToken: token, // Optional
	//}
	//
	//firstTimestampCurl := curlParamFirstTS.GenerateCurlCommand()

	//chunkSize := int64(chunkDays * 24 * 60 * 60 * 1e9)

	curlParamEndTS := CurlRequest{
		BaseURL: "query_range",
		Params: map[string]string{
			"query":     `{pod=~".+"}`,
			"limit":     "1",
			"direction": "BACKWARD",
		},
		AuthToken: token, // Optional
	}

	endDumpTimestampCurl := curlParamEndTS.GenerateCurlCommand()
	endDumpTimestampJson, _, err := getLogTimestamp(config, kubeCl, endDumpTimestampCurl)
	if err != nil {
		return fmt.Errorf("Error get latest timestamp JSON from Loki: %s", err)
	}
	endDumpTimestamp, err := strconv.ParseInt(endDumpTimestampJson.Data.Result[0].Values[0][0], 10, 64)
	if err != nil {
		return fmt.Errorf("Error converting timestamp:", err)
	}

	fmt.Printf("%v\n", endDumpTimestamp)

	curlParamStreamList := CurlRequest{
		BaseURL:   "series",
		AuthToken: token, // Optional
	}
	streamListDumpCurl := curlParamStreamList.GenerateCurlCommand()

	_, streamListDumpJson, err := getLogTimestamp(config, kubeCl, streamListDumpCurl)
	if err != nil {
		return fmt.Errorf("Error get stream list JSON from Loki: %s", err)
	}
	//streamListDump, err := strconv.ParseInt(streamListDumpJson.Data.Result[0].Values[0][0], 10, 64)
	//if err != nil {
	//	return fmt.Errorf("Error converting timestamp:", err)
	//}

	//var wg sync.WaitGroup
	//logsChan := make(chan []string, parallelJobs) // Buffered channel for parallel execution
	//sem := make(chan struct{}, parallelJobs)      // Semaphore to limit parallel requests

	// Fetch logs in reverse order (end -> start)
	//var startTime int64
	//for t := endDumpTimestamp; t > startTime; t -= int64(chunkSize) {
	//var lokiResp LokiResponse
	for _, result := range streamListDumpJson.Data {
		for podName := range result.Pod {
			containerNameStream := result.Container
			fmt.Printf("Pod name is %v\nContainer name is : %s", podName, containerNameStream)

			//curlParamStream := CurlRequest{
			//	BaseURL: "query_range",
			//	Params: map[string]string{
			//		//"query":     `{pod=~"podName"}`,
			//		"query": strconv.Itoa(podName),
			//		"limit":     "1",
			//		"direction": "BACKWARD",
			//	},
			//	AuthToken: token, // Optional
			//}
			//
			//endDumpTimestampCurl := curlParamEndTS.GenerateCurlCommand()
			//endDumpTimestampJson, _, err := getLogTimestamp(config, kubeCl, endDumpTimestampCurl)
			//if err != nil {
			//	return fmt.Errorf("Error get latest timestamp JSON from Loki: %s", err)
			//}
			//endDumpTimestamp, err := strconv.ParseInt(endDumpTimestampJson.Data.Result[0].Values[0][0], 10, 64)
			//if err != nil {
			//	return fmt.Errorf("Error converting timestamp:", err)
			//}
		}

	}

	//for t := endDumpTimestamp; len(result.Data.Result) > startTime; t -= int64(chunkSize) {
	//	startTime := endDumpTimestamp - chunkSize
	//
	//	wg.Add(1)
	//
	//	go fetchLogs(t-int64(chunkSize), t, &wg, sem, logsChan)
	//}

	//for t := endDumpTimestamp; len(result.Data.Result) > startTime; t -= int64(chunkSize) {
	//	startTime := endDumpTimestamp - chunkSize
	//
	//	wg.Add(1)
	//
	//	go fetchLogs(t-int64(chunkSize), t, &wg, sem, logsChan)
	//}
	//
	//go func() {
	//	wg.Wait()
	//	close(logsChan)
	//}()

	//if len(result.Data.Result) > 0 && len(result.Data.Result[0].Values) > 0 {
	//	//return result.Data.Result[0].Values[0][0], nil // First log's timestamp
	//	//startTS = result.Data.Result[0].Values[0][0] // First log's timestamp
	//	startTS, err = strconv.ParseInt(result.Data.Result[0].Values[0][0], 10, 64) // First log's timestamp
	//	if err != nil {
	//		return fmt.Errorf("Error converting timestamp:", err)
	//	}
	//}

	////results := make(chan LokiResponse, parallelJobs)
	//// Iterate over chunks of 30 days
	//for chunkEnd := endTimestampBuff; chunkStart < chunkEnd; chunkStart -= chunkSize {
	//	//chunkEnd := chunkStart + chunkSize
	//	//if chunkEnd > endUnix {
	//	//	chunkEnd = endUnix
	//	//}
	//	wg.Add(1)
	//	sem <- struct{}{} // Acquire a slot
	//	chunkStart := chunkEnd - chunkSize
	//	//
	//	curlParamStartTS := CurlRequest{
	//		Params: map[string]string{
	//			"query":     `{pod=~".+"}`,
	//			"limit":     "1",
	//			"direction": "FORWARD",
	//			"start":     chunkStart,
	//			"end":       chunkEnd,
	//		},
	//		AuthToken: token, // Optional
	//	}
	//	//
	//	//startTimestampCurl := curlParamStartTS.GenerateCurlCommand()
	//	//startTimestampBuff, err := getLogTimestamp(config, kubeCl, startTimestampCurl)
	//	//if err != nil {
	//	//	return fmt.Errorf("Error get latest timestamp from Loki: %s", err)
	//	//}
	//	go func(start, end int64) {
	//		defer func() { <-sem }() // Release a slot
	//		fetchLogs(start, end, &wg, results)
	//	}(chunkStart, chunkEnd)
	//
	//}

	// Close results channel once all goroutines finish
	//go func() {
	//	wg.Wait()
	//	close(results)
	//}()

	//executor, err := ExecInPod(config, kubeCl, latestTimestamp, podName, namespaceDeckhouse, containerName)
	//if err = executor.StreamWithContext(
	//	context.Background(),
	//	remotecommand.StreamOptions{
	//		Stdout: &stdout,
	//		Stderr: &stderr,
	//	}); err != nil {
	//	fmt.Fprintf(os.Stderr, strings.Join(latestTimestamp, " "))
	//	fmt.Fprintf(os.Stderr, stderr.String())
	//}

	//if err = cmd.Writer(tarWriter, stdout.Bytes())
	// err != nil {
	//	return fmt.Errorf("failed to update the %s", err)
	//}
	//fmt.Printf("loki url is %s\n", firstTimestampCurl)
	//fmt.Printf("loki url is %s\n", latestTimestamp)
	//fmt.Printf("%v\n%v\n", endDumpTimestamp, streamListDump)
	//fmt.Printf("%s\n", stderr.String())
	//var stdout bytes.Buffer
	//err = json.Unmarshal(stdout.Bytes(), &result)
	//if err != nil {
	//	return fmt.Errorf("failed unmarshal %s", err)
	//}

	//var logs string
	//for _, resultLog := range result.Data.Result {
	//	for _, log := range resultLog.Values {
	//		//fmt.Sprintf("Pod: %s\nTimestamp: %s, Log: %s\n", pod, log[0], log[1])
	//		fmt.Printf("Pod: %s, Container: %s, Timestamp: %s, Log: %s\n", resultLog.Stream.Pod, resultLog.Stream.Container, log[0], log[1])
	//	}
	//}

	//fmt.Printf("%s\n", logs)

	//fmt.Fprintf(os.Stdout, stderr.String())

	//logs, err := fetchLokiLogs(chunkStart, chunkEnd)
	//if err != nil {
	//	return fmt.Errorf("Error fetching logs: %v\n", err)
	//}

	return err
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

func ExecInPod(config *rest.Config, kubeCl kubernetes.Interface, getApi []string, podName string, namespace string, containerName string) (remotecommand.Executor, error) {
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
			Command:   getApi,
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

//func Writer(tarWriter *tar.Writer, fileContent []byte) error {
//	header := &tar.Header{
//		Name: string(),
//		Mode: 0o600,
//		Size: int64(len(fileContent)),
//	}
//
//	if err := tarWriter.WriteHeader(header); err != nil {
//		return fmt.Errorf("write tar header: %v", err)
//	}
//
//	if _, err := tarWriter.Write(fileContent); err != nil {
//		return fmt.Errorf("copy content: %v", err)
//	}
//
//	return nil
//}

type LokiQuery interface {
	GetType() string
}

func getLogTimestamp(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*LokiResponse, *SeriesApi, error) {
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
			//fmt.Printf("condition match\n %s is %s/series\n", t, lokiURL)
			var result SeriesApi
			err = json.Unmarshal(stdout.Bytes(), &result)
			if err != nil {
				return nil, nil, fmt.Errorf("failed unmarshal SeriesApi %s", err)
			}
			return nil, &result, nil
		}
		var result LokiResponse
		err = json.Unmarshal(stdout.Bytes(), &result)
		if err != nil {
			return nil, nil, fmt.Errorf("failed unmarshal  LokiResponse%s", err)
		}
		return &result, nil, nil
	}

	return nil, nil, nil
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

//func fetchLogs(start, end int64, wg *sync.WaitGroup, sem chan struct{}, logsChan chan []string) {
//	defer wg.Done()
//	sem <- struct{}{}        // Acquire semaphore slot
//	defer func() { <-sem }() // Release slot when done
//
//	//url := fmt.Sprintf("%s/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=5000&direction=backward", lokiURL, query, start, end)
//	resp, err := http.Get(url)
//	if err != nil {
//		fmt.Println("Request failed:", err)
//		return
//	}
//	defer resp.Body.Close()
//
//	body, err := io.ReadAll(resp.Body)
//	if err != nil {
//		fmt.Println("Failed to read response:", err)
//		return
//	}
//
//	var lokiResp LokiResponse
//	if err := json.Unmarshal(body, &lokiResp); err != nil {
//		fmt.Println("JSON parse error:", err)
//		return
//	}
//
//	var logs []string
//	for _, result := range lokiResp.Data.Result {
//		for _, entry := range result.Values {
//			timestamp := entry[0]
//			logMessage := entry[1]
//			logs = append(logs, fmt.Sprintf("[%s] %s", timestamp, logMessage))
//		}
//	}
//
//	logsChan <- logs // Send logs to channel
//}
