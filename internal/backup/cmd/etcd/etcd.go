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

package etcd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/samber/lo"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var etcdLong = templates.LongDesc(`
Take a snapshot of ETCD state.
		
This command creates a snapshot of the Kubernetes underlying key-value database ETCD.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	etcdCmd := &cobra.Command{
		Use:           "etcd <snapshot-path>",
		Short:         "Take a snapshot of ETCD state",
		Long:          etcdLong,
		ValidArgs:     []string{"snapshot-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return validateFlags(cmd)
		},
		RunE: etcd,
	}

	addFlags(etcdCmd.Flags())
	return etcdCmd
}

const (
	etcdPodNamespace      = "kube-system"
	etcdPodsLabelSelector = "component=etcd"

	bufferSize16MB = 16 * 1024 * 1024
)

var (
	requestedEtcdPodName string

	verboseLog bool
)

func etcd(cmd *cobra.Command, args []string) error {
	log.SetFlags(log.LstdFlags)
	if len(args) != 1 {
		return fmt.Errorf("This command requires exactly 1 argument")
	}

	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	etcdPods, err := findETCDPods(kubeCl)
	if err != nil {
		return fmt.Errorf("Looking up etcd pods failed: %w", err)
	}

	pipeExecOpts := &v1.PodExecOptions{
		Stdout:    true,
		Stderr:    true,
		Container: "etcd",
		Command: []string{
			"/usr/bin/etcdctl",
			"--endpoints", "https://127.0.0.1:2379/",
			"--key", "/etc/kubernetes/pki/etcd/ca.key",
			"--cert", "/etc/kubernetes/pki/etcd/ca.crt",
			"--cacert", "/etc/kubernetes/pki/etcd/ca.crt",
			"snapshot", "pipe",
		},
	}

	if len(etcdPods) > 1 {
		log.Println(
			"Will try to snapshot these instances sequentially until one of them succeeds or all of them fail",
			etcdPods)
	}

	for _, etcdPodName := range etcdPods {
		log.Println("Trying to snapshot", etcdPodName)

		snapshotFile, err := os.CreateTemp(".", ".*.snapshotPart")
		if err != nil {
			return fmt.Errorf("Failed to prepare temporary etcd snapshot file: %w", err)
		}
		defer func(fileName string) {
			_ = os.Remove(fileName)
		}(snapshotFile.Name())

		stdout := bufio.NewWriterSize(snapshotFile, bufferSize16MB)
		stderr := &bytes.Buffer{}

		if err = checkEtcdPodExistsAndReady(kubeCl, etcdPodName); err != nil {
			log.Printf("%s: Fail, %v\n", etcdPodName, err)
			continue
		}

		snapshotStreamingSupported, err := checkEtcdInstanceSupportsSnapshotStreaming(kubeCl, config, etcdPodName)
		if err != nil {
			log.Printf("%s: Fail, %v\n", etcdPodName, err)
			continue
		}
		if !snapshotStreamingSupported {
			log.Printf("%s: etcd instance does not support snapshot streaming\n", etcdPodName)
			continue
		}

		if err = streamCommand(kubeCl, config, pipeExecOpts, etcdPodName, etcdPodNamespace, stdout, stderr); err != nil {
			log.Printf("%s: Fail, %v\n", etcdPodName, err)
			if verboseLog {
				log.Println("STDERR:", stderr.String())
			}
			continue
		}

		if err = stdout.Flush(); err != nil {
			return fmt.Errorf("Flushing snapshot data to disk: %w", err)
		}

		if err = os.Rename(snapshotFile.Name(), args[0]); err != nil {
			return fmt.Errorf("Failed to move snapshot file: %w", err)
		}

		log.Println("Snapshot successfully taken from", etcdPodName)
		return nil
	}

	return fmt.Errorf("All known etcd replicas are unavailable to snapshot")
}

func checkEtcdInstanceSupportsSnapshotStreaming(
	kubeCl *kubernetes.Clientset,
	config *rest.Config,
	etcdPodName string,
) (bool, error) {
	helpExecOpts := &v1.PodExecOptions{
		Stdout:    true,
		Stderr:    true,
		Container: "etcd",
		Command: []string{
			"/usr/bin/etcdctl", "help",
		},
	}

	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	if err := streamCommand(kubeCl, config, helpExecOpts, etcdPodName, etcdPodNamespace, stdout, stderr); err != nil {
		if verboseLog {
			log.Println("HELP STDERR:", stderr.String())
		}
		return false, fmt.Errorf("streamCommand: %w", err)
	}

	if bytes.Contains(stdout.Bytes(), []byte("snapshot pipe")) {
		return true, nil
	}

	return false, nil
}

func streamCommand(
	kubeCl kubernetes.Interface,
	restConfig *rest.Config,
	execOpts *v1.PodExecOptions,
	podName, podNamespace string,
	stdout, stderr io.Writer,
) error {
	scheme := runtime.NewScheme()
	parameterCodec := runtime.NewParameterCodec(scheme)
	if err := v1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("Failed to create parameter codec: %w", err)
	}

	request := kubeCl.CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		SubResource("exec").
		VersionedParams(execOpts, parameterCodec).
		Namespace(podNamespace).
		Name(podName)

	executor, err := remotecommand.NewSPDYExecutor(restConfig, "POST", request.URL())
	if err != nil {
		log.Printf("Creating SPDY executor for Pod %s: %v", podName, err)
	}

	if err = executor.StreamWithContext(
		context.Background(),
		remotecommand.StreamOptions{
			Stdout: stdout,
			Stderr: stderr,
		}); err != nil {
		return err
	}

	return nil
}

func findETCDPods(kubeCl kubernetes.Interface) ([]string, error) {
	if requestedEtcdPodName != "" {
		if err := checkEtcdPodExistsAndReady(kubeCl, requestedEtcdPodName); err != nil {
			return nil, err
		}

		return []string{requestedEtcdPodName}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pods, err := kubeCl.CoreV1().Pods(etcdPodNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: etcdPodsLabelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("listing etcd Pods: %w", err)
	}

	pods.Items = lo.Filter(pods.Items, func(pod v1.Pod, _ int) bool {
		podIsReady := lo.FindOrElse(
			pod.Status.Conditions, v1.PodCondition{},
			func(condition v1.PodCondition) bool {
				return condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue
			}).Status == v1.ConditionTrue

		_, foundEtcdContainer := lo.Find(pod.Spec.Containers, func(container v1.Container) bool {
			return container.Name == "etcd"
		})

		return podIsReady && foundEtcdContainer
	})

	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no valid etcd Pods found")
	}

	return lo.Map(pods.Items, func(pod v1.Pod, _ int) string {
		return pod.Name
	}), nil
}

func checkEtcdPodExistsAndReady(kubeCl kubernetes.Interface, podName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pod, err := kubeCl.CoreV1().Pods(etcdPodNamespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Query Pod %s: %w", podName, err)
	}

	podReady := lo.FindOrElse(pod.Status.Conditions, v1.PodCondition{}, func(condition v1.PodCondition) bool {
		return condition.Type == v1.PodReady
	}).Status == v1.ConditionTrue

	if !podReady {
		return fmt.Errorf("Pod %s is not yet ready, cannot snapshot it now", podName)
	}

	return nil
}
