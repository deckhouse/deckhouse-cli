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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

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
	parallelJobs = 1             // Number of parallel requests
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
			//Stream map[string]string `json:"stream"`
			Values [][]string `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

type SeriesApi struct {
	Data []map[string]string `json:"data"`
}

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

	token := "eyJhbGciOiJSUzI1NiIsImtpZCI6IkFnbVRCVndWRm43dy04Qmg1cENqcXFQMVFhOEhuLXF0dUpFSTdWQXBYYUkifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkOC1tb25pdG9yaW5nIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6Imxva2ktYXBpLXRva2VuIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6Imxva2kiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiI0N2Y2ZWY1Ni01YjdkLTRlNjUtYTc3Zi1mNTI0ODkyZDJhNzgiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6ZDgtbW9uaXRvcmluZzpsb2tpIn0.EF-RGqY0acC-C_2KPz51UPdwkLMGw-DV2nsrJuh2lQZ_0ebiwTmWoVFCj6o7Ey2z9CsNHkvEr9jxTc7uHh0rvRQIJp5rUrimeSBfvrJpLaiiVQ_h5cXJN84l5jq4IkbzO7lUObtjh6DmNzodZCbxMEu-Gm766weRhUdoW8zco7Cd-m26sQK4095tp9_4iW5lXBGC6R68DEa-2pjZjHpDspRwnI4XY_BVXldaIKpbR5cCU-8CKzJ0BXSvDcjKUjFv3Mk0TomMSFSlnMY5wyvr6vvus11E3MxajRq1vL9PJiW1ZfBFRnwEQsQnsIPgQMb45fmpgayCLMBnmjNF4WRxvg"

	chunkSize := int64(chunkDays * 24 * 60 * 60 * 1e9) //30 days in nanosec timestamp

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

	for chunkEnd := endDumpTimestamp; chunkEnd > 0; chunkEnd -= chunkSize {
		chunkStart := chunkEnd - chunkSize

		fmt.Printf("Fetching logs from %v to %v\n", chunkStart, chunkEnd)

		curlParamStreamList := CurlRequest{
			BaseURL: "series",
			Params: map[string]string{
				"end":   strconv.FormatInt(chunkEnd, 10),
				"start": strconv.FormatInt(chunkStart, 10),
				"match": `{pod=~"loki-0", container=~"kube-rbac-proxy"}`,
			},
			AuthToken: token, // Optional
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

		var wg sync.WaitGroup
		sem := make(chan struct{}, parallelJobs) // Semaphore to limit concurrent requests
		//logsChan := make(chan map[string][]string, parallelJobs)
		logsChan := make(chan []string, parallelJobs)
		//podContainerLogs := make(map[string][]string)
		errChan := make(chan error, parallelJobs)

		for _, result := range streamListDumpJson.Data {

			wg.Add(1)
			go fetchLogs(chunkStart, chunkEnd, endDumpTimestamp, token, result, config, kubeCl, &wg, sem, logsChan, errChan)

		}

		go func() {
			wg.Wait()
			close(logsChan)
			close(errChan)
		}()

		var allLogs []string
		for chunk := range logsChan {
			allLogs = append(allLogs, chunk...)
			fmt.Printf("\nLogs: %s", allLogs)
		}

		////// Collect errors from channel
		//var errorsList []error
		//for err := range errChan {
		//	errorsList = append(errorsList, err)
		//}
		//
		//if len(errChan) > 0 {
		//	for err := range errChan {
		//		fmt.Println(err)
		//	}
		//}
		//
		//// Collect logs from all goroutines
		////podContainerLogs := make(map[string][]string)
		//for result := range logsChan {
		//	for key, logs := range result {
		//		podContainerLogs[key] = append(podContainerLogs[key], logs...)
		//	}
		//}
		//
		////// Save logs to files
		////var logFiles []string
		//for key, logs := range podContainerLogs {
		//	//filename := fmt.Sprintf("%s.log", key)
		//	fmt.Printf("Pod, Container: %s\n Logs: %s\n", key, logs)
		//
		//	//if err := writeLogsToFile(filename, logs); err == nil {
		//	//	logFiles = append(logFiles, filename)
		//	//}
		//}
		//
		//// Compress logs into tar.gz
		//if err := createTarGz(logFiles, "logs.tar.gz"); err == nil {
		//	fmt.Println("✅ Logs compressed to logs.tar.gz")
		//}
	}

	return err
}

func fetchLogs(chunkStart, chunkEnd, endDumpTimestamp int64, token string, result1 map[string]string, config *rest.Config, kubeCl kubernetes.Interface, wg *sync.WaitGroup, sem chan struct{}, logsChan chan []string, errChan chan error) {
	defer wg.Done()
	sem <- struct{}{}        // Acquire semaphore slot
	defer func() { <-sem }() // Release slot when done

	containerNameStream, _ := result1["container"]
	podNameStream, _ := result1["pod"]

	//fmt.Printf("STREAM IS: Pod name is %v , Container name is : %s\n", podNameStream, containerNameStream)

	query1 := fmt.Sprintf(`{pod=~"%s", container=~"%s"}`, podNameStream, containerNameStream)

	chunkEnd = endDumpTimestamp
	//if hadContainer {}
	for chunkEnd > chunkStart {

		fmt.Printf("Fetch logs for pod: %s and container: %s in time range chunkStart: %v and chunkEnd: %v", podNameStream, containerNameStream, chunkStart, chunkEnd)

		curlParamDumpLog := CurlRequest{
			BaseURL: "query_range",
			Params: map[string]string{
				"end":       strconv.FormatInt(chunkEnd, 10),
				"start":     strconv.FormatInt(chunkStart, 10),
				"query":     query1,
				"limit":     "5000",
				"direction": "BACKWARD",
			},
			AuthToken: token, // Optional
		}
		DumpLogCurl := curlParamDumpLog.GenerateCurlCommand()
		DumpLogCurlJson, _, err := getLogTimestamp(config, kubeCl, DumpLogCurl)
		if err != nil {
			errChan <- fmt.Errorf("Error get JSON from Loki: %s", err)
		}

		fmt.Printf("chunkStart is: %v , chunkEnd : %v\n", chunkStart, chunkEnd)

		if len(DumpLogCurlJson.Data.Result) == 0 {
			fmt.Printf("No more logs.\nStop...\n")
			break
		}

		//logsByPodContainer := make(map[string][]string)
		var logs []string
		for _, result2 := range DumpLogCurlJson.Data.Result {
			for _, entry := range result2.Values {
				timestampInt64, err := strconv.ParseInt(entry[0], 10, 64)
				if err != nil {
					errChan <- fmt.Errorf("Error converting timestamp:", err)
				}
				timestampUtc := time.Unix(0, timestampInt64).UTC()
				//fileKey := fmt.Sprintf("%s-%s", podNameStream, containerNameStream)
				//logs := fmt.Sprintf("Timestamp: [%v], Log: %s\n", timestampUtc, entry[1])
				logs = append(logs, fmt.Sprintf("Timestamp: [%v], Log: %s\n", timestampUtc, entry[1]))
				//fmt.Printf("%s", logs)
				//logsByPodContainer[fileKey] = append(logsByPodContainer[fileKey], logs)
				//for key, logs1 := range logsByPodContainer {
				//	fmt.Printf("Pod, Container: \n%s\n Logs: \n%s\n", key, logs1)
				//}

			}
		}

		//logsChan <- logsByPodContainer // Send logs to channel
		logsChan <- logs // Send logs to channel

		firstLog := DumpLogCurlJson.Data.Result[len(DumpLogCurlJson.Data.Result)-1].Values[len(DumpLogCurlJson.Data.Result[len(DumpLogCurlJson.Data.Result)-1].Values)-1][0]
		firstTimestamp, err := strconv.ParseInt(firstLog, 10, 64)
		if err != nil {
			errChan <- fmt.Errorf("Error converting timestamp:", err)
		}
		//fmt.Println("Fetching next batch from:", firstTimestamp)
		chunkEnd = firstTimestamp
	}
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
		} else if t == fmt.Sprintf("%s/query_range", lokiURL) {
			//fmt.Printf("condition match\n %s is %s/query_range\n", t, lokiURL)
			var result LokiResponse
			err = json.Unmarshal(stdout.Bytes(), &result)
			if err != nil {
				return nil, nil, fmt.Errorf("failed unmarshal LokiResponse%s", err)
			}
			return &result, nil, nil
		}
		stdout.Reset()
	}

	return nil, nil, nil
}

// Create a tar.gz file from log files
func createTarGz(logFiles []string, outputFile string) error {
	tarFile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer tarFile.Close()

	gzWriter := gzip.NewWriter(tarFile)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	for _, logFile := range logFiles {
		file, err := os.Open(logFile)
		if err != nil {
			return err
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		if _, err := io.Copy(tarWriter, file); err != nil {
			return err
		}
	}

	return nil
}

// Write logs to a file
func writeLogsToFile(filename string, logs []string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, log := range logs {
		_, err := file.WriteString(log + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}
