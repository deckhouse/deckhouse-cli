/*
Copyright 2025 Flant JSC

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

package queuefetcher

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	namespace        = "d8-system"
	labelSelector    = "app=deckhouse,leader=true"
	containerName    = "deckhouse"
	controllerBinary = "deckhouse-controller"
)

// QueueFetcher encapsulates access to a queue
type QueueFetcher interface {
	GetQueue(ctx context.Context) (DeckhouseQueue, error)
}

type DeckhouseQueueFetcher struct {
	kubeCl     kubernetes.Interface
	restConfig *rest.Config
}

func New(kubeCl kubernetes.Interface, restConfig *rest.Config) *DeckhouseQueueFetcher {
	return &DeckhouseQueueFetcher{kubeCl: kubeCl, restConfig: restConfig}
}

type DeckhouseQueue struct {
	Header  string
	Tasks   []DeckhouseQueueTask
	Summary []string
}

type DeckhouseQueueTask struct {
	Index int
	Text  string
}

// GetQueue fetches and processes queue from Deckhouse leader
func (q *DeckhouseQueueFetcher) GetQueue(ctx context.Context) (DeckhouseQueue, error) {
	podName, err := q.findLeaderPod(ctx)
	if err != nil {
		return DeckhouseQueue{}, err
	}

	output, err := q.execQueueList(ctx, podName)
	if err != nil {
		return DeckhouseQueue{}, err
	}

	return processDeckhouseQueue(output), nil
}

func (q *DeckhouseQueueFetcher) findLeaderPod(ctx context.Context) (string, error) {
	pods, err := q.kubeCl.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no Deckhouse leader pods found with label %s", labelSelector)
	}
	return pods.Items[0].Name, nil
}

func (q *DeckhouseQueueFetcher) execQueueList(_ context.Context, podName string) (string, error) {
	req := q.kubeCl.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		Param("container", containerName).
		Param("stdin", "false").
		Param("stdout", "true").
		Param("stderr", "true").
		Param("tty", "false").
		Param("command", controllerBinary).
		Param("command", "queue").
		Param("command", "list")

	exec, err := remotecommand.NewSPDYExecutor(q.restConfig, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("failed to initialize SPDY executor: %w", err)
	}
	var stdout, stderr strings.Builder
	err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return "", fmt.Errorf("failed to execute command: %w", err)
	}
	if stderr.Len() > 0 {
		return "", fmt.Errorf("stderr: %s", stderr.String())
	}
	return stdout.String(), nil
}

// processDeckhouseQueue parses output to internal struct
func processDeckhouseQueue(raw string) DeckhouseQueue {
	lines := strings.Split(strings.TrimRight(raw, "\n"), "\n")
	header := ""
	var tasks []DeckhouseQueueTask
	var summary []string
	inSummary := false
	taskRegexp := regexp.MustCompile(`^(\d+)\.\s+(.+)$`)

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch {
		case i == 0 && strings.HasPrefix(line, "Queue "):
			header = line
		case strings.HasPrefix(line, "Summary:"):
			inSummary = true
			summary = append(summary, line)
		case inSummary:
			summary = append(summary, line)
		case taskRegexp.MatchString(line):
			m := taskRegexp.FindStringSubmatch(line)
			index, _ := strconv.Atoi(m[1])
			tasks = append(tasks, DeckhouseQueueTask{
				Index: index,
				Text:  m[0],
			})
		default:
			summary = append(summary, line)
		}
	}
	return DeckhouseQueue{
		Header:  header,
		Tasks:   tasks,
		Summary: summary,
	}
}
