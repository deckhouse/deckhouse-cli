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
	"strings"
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

//const (
//	etcdPodNamespace      = "kube-system"
//	etcdPodsLabelSelector = "component=etcd"
//
//	bufferSize16MB = 16 * 1024 * 1024
//)
//
//var (
//	requestedEtcdPodName string
//
//	verboseLog bool
//)

const (
	lokiURL = "https://loki.d8-monitoring.svc.cluster.local:3100/loki/api/v1/query_range"
	//lokiURL      = "https://loki.d8-monitoring.svc.cluster.local:3100/loki/api/v1/series"
	parallelJobs = 1                      // Number of parallel requests
	query        = `query={pod=~".+"}`    // LogQL query
	startTime    = "2025-02-12T16:22:00Z" // Start time
	endTime      = "2025-02-12T16:25:00Z" // End time
	limit        = `limit=5000`           // Number of logs per query
	direction    = `direction=FORWARD`
)

// LokiResponse Struct to store API response
type LokiResponse struct {
	Data struct {
		Result []struct {
			Stream map[string]string `json:"stream"`
			Values [][]string        `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

//type Command struct {
//	Cmd  string
//	Args []string
//	File string
//}

func backupLoki(cmd *cobra.Command, _ []string) error {

	//req := client.Get().RequestURI("")

	//err = createtarball.Tarball(config, kubeCl)
	//if err != nil {
	//	return fmt.Errorf("Error collecting debug info: %w", err)
	//}
	const (
		namespace   = "d8-monitoring" // Change to your service namespace
		serviceName = "loki:"         // Change to your service name
		portScheme  = "https:"
		servicePort = "3100" // Change to the service port name
		//namespace   = "default"      // Change to your service namespace
		//serviceName = "log-service:" // Change to your service name
		//portScheme  = "http:"
		//servicePort = "80" // Change to the service port name
		labelSelector      = "leader=true"
		namespaceDeckhouse = "d8-system"
		containerName      = "deckhouse"
	)
	//loki.d8-monitoring.svc.cluster.local:3100
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	podName, err := GetDeckhousePod(kubeCl, namespaceDeckhouse, labelSelector)

	var stdout, stderr bytes.Buffer

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
	//}

	// Convert time range to Unix timestamps
	startTS, _ := time.Parse(time.RFC3339, startTime)
	endTS, _ := time.Parse(time.RFC3339, endTime)
	totalDuration := endTS.Sub(startTS)

	// Calculate chunk size (divide total duration by parallel jobs)
	chunkSize := totalDuration / time.Duration(parallelJobs)

	chunkStart := startTS.Add(time.Duration(1) * chunkSize)
	chunkEnd := chunkStart.Add(chunkSize)

	fmt.Printf("Fetching logs from %s to %s\n", chunkStart, chunkEnd)

	var result LokiResponse

	// Build Loki query parameters
	//queryParams := url.Values{}
	//queryParams.Set("query", query)
	//queryParams.Set("start", fmt.Sprintf("%d", chunkStart.UnixNano()))
	//queryParams.Set("end", fmt.Sprintf("%d", chunkEnd.UnixNano()))
	//queryParams.Set("limit", limit)

	//reqURL := fmt.Sprintf("curl -vs '%s%s'", lokiURL, queryParams.Encode())

	//fullEndpointUrl := fmt.Sprintf("%s://%s:%s/%s/%s", apiProtocol, apiEndpoint, apiPort, queuePath, pathFromOption)

	token := "eyJhbGciOiJSUzI1NiIsImtpZCI6IkFnbVRCVndWRm43dy04Qmg1cENqcXFQMVFhOEhuLXF0dUpFSTdWQXBYYUkifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkOC1tb25pdG9yaW5nIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6Imxva2ktYXBpLXRva2VuIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6Imxva2kiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiI0N2Y2ZWY1Ni01YjdkLTRlNjUtYTc3Zi1mNTI0ODkyZDJhNzgiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6ZDgtbW9uaXRvcmluZzpsb2tpIn0.EF-RGqY0acC-C_2KPz51UPdwkLMGw-DV2nsrJuh2lQZ_0ebiwTmWoVFCj6o7Ey2z9CsNHkvEr9jxTc7uHh0rvRQIJp5rUrimeSBfvrJpLaiiVQ_h5cXJN84l5jq4IkbzO7lUObtjh6DmNzodZCbxMEu-Gm766weRhUdoW8zco7Cd-m26sQK4095tp9_4iW5lXBGC6R68DEa-2pjZjHpDspRwnI4XY_BVXldaIKpbR5cCU-8CKzJ0BXSvDcjKUjFv3Mk0TomMSFSlnMY5wyvr6vvus11E3MxajRq1vL9PJiW1ZfBFRnwEQsQnsIPgQMb45fmpgayCLMBnmjNF4WRxvg"

	curlEndpointUrl := fmt.Sprintf("Authorization: Bearer %s", token)
	fullEndpointUrl := fmt.Sprintf("%s", lokiURL)
	//encodeUrl := fmt.Sprintf("--data-urlencode 'start=%v' --data-urlencode 'end=%v' --data-urlencode 'query=%s' --data-urlencode 'limit=%s' --data-urlencode 'direction=%s'", chunkStart.UnixNano(), chunkEnd.UnixNano(), query, limit, direction)
	//encodeUrl := fmt.Sprintf("--data-urlencode 'query=%s' --data-urlencode 'limit=%s' --data-urlencode 'direction=%s'", query, limit, direction)
	//fullCommand := []string{"curl", "-v", "--insecure", "-H", curlEndpointUrl, fullEndpointUrl, encodeUrl}
	fullCommand := []string{"curl", "-v", "--insecure", "-H", curlEndpointUrl, fullEndpointUrl, "--data-urlencode", fmt.Sprintf("%s", query), "--data-urlencode", fmt.Sprintf("%s", limit), "--data-urlencode", fmt.Sprintf("%s", direction)}

	//fullCommand := []string{"curl"}

	executor, err := ExecInPod(config, kubeCl, fullCommand, podName, namespaceDeckhouse, containerName)
	if err = executor.StreamWithContext(
		context.Background(),
		remotecommand.StreamOptions{
			Stdout: &stdout,
			Stderr: &stderr,
		}); err != nil {
		fmt.Fprintf(os.Stderr, strings.Join(fullCommand, " "))
		fmt.Fprintf(os.Stderr, stderr.String())
	}

	//if err = cmd.Writer(tarWriter, stdout.Bytes())
	// err != nil {
	//	return fmt.Errorf("failed to update the %s", err)
	//}
	fmt.Printf("loki url is %s\n", fullCommand)
	//fmt.Fprintf(os.Stdout, stdout.String())
	//fmt.Printf("%s\n", stdout.String())

	err = json.Unmarshal(stdout.Bytes(), &result)
	if err != nil {
		return fmt.Errorf("failed unmarshal %s", err)
	}

	fmt.Printf("%s\n", result)

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

//func fetchLokiLogs(start, end time.Time) (LokiResponse, error) {
//	var result LokiResponse
//
//	// Build Loki query parameters
//	queryParams := url.Values{}
//	queryParams.Set("query", query)
//	queryParams.Set("start", fmt.Sprintf("%d", start.UnixNano()))
//	queryParams.Set("end", fmt.Sprintf("%d", end.UnixNano()))
//	queryParams.Set("limit", limit)
//
//	reqURL := fmt.Sprintf("%s?%s", lokiURL, queryParams.Encode())
//
//	//// Execute HTTP GET request (similar to cURL)
//	//resp, err := http.Get(reqURL)
//	//if err != nil {
//	//	return result, err
//	//}
//	//defer resp.Body.Close()
//	//
//	//if resp.StatusCode != 200 {
//	//	return result, fmt.Errorf("Loki API error: %s", resp.Status)
//	//}
//	//
//	//body, err := io.ReadAll(resp.Body)
//	//if err != nil {
//	//	return result, err
//	//}
//
//	// Parse Loki JSON response
//	err = json.Unmarshal(body, &result)
//	if err != nil {
//		return result, err
//	}
//
//	return result, nil
//}
