package utilk8s

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

func GetDeckhousePod(kubeCl kubernetes.Interface) (string, error) {
	pods, err := kubeCl.CoreV1().Pods("d8-system").List(context.Background(), metav1.ListOptions{
		LabelSelector: "leader=true",
	})
	if err != nil {
		return "", fmt.Errorf("error listing pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pods deckhouse available in namespace d8-system")
	}

	pod := pods.Items[0]
	podName := pod.Name

	return podName, nil
}

func ExecInPod(config *rest.Config, kubeCl kubernetes.Interface, cmdLine []string, podName string, namespace string, containerName string) (remotecommand.Executor, error) {
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
			Command:   cmdLine,
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

// ExecCommandInPod runs cmdLine in the given container and returns whatever the
// command wrote to stdout and stderr. Output collected before an error (a
// timeout in particular) is returned along with that error, so callers can keep
// a partial result.
//
// The buffers are goroutine-safe on purpose: StreamWithContext returns as soon
// as ctx is done without joining the goroutines that copy the remote streams,
// so those goroutines may still write into them after this call returned.
func ExecCommandInPod(
	ctx context.Context,
	config *rest.Config,
	kubeCl kubernetes.Interface,
	cmdLine []string,
	podName, namespace, containerName string,
) ([]byte, string, error) {
	executor, err := ExecInPod(config, kubeCl, cmdLine, podName, namespace, containerName)
	if err != nil {
		return nil, "", err
	}

	var stdoutBuf, stderrBuf syncBuffer

	streamErr := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdoutBuf,
		Stderr: &stderrBuf,
	})
	if streamErr != nil {
		return stdoutBuf.Bytes(), stderrBuf.String(), streamErr
	}

	return stdoutBuf.detach(), stderrBuf.String(), nil
}

// syncBuffer is a goroutine-safe sink for the output of a remote command.
//
// It is needed because remotecommand.Executor.StreamWithContext returns as
// soon as the context is done (client-go tools/remotecommand/spdy.go) without
// joining the goroutines that io.Copy the remote streams into the writers
// passed in StreamOptions. After a command times out those goroutines may keep
// writing, so the writer outlives the call and a plain bytes.Buffer would be
// accessed concurrently: a racing Bytes() can return a slice already grown past
// the bytes actually copied into it, and a late Write can resurrect a buffer the
// caller considers finished.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements io.Writer and is safe to call concurrently with the readers
// below.
func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

// Bytes returns a copy of everything written so far. The copy keeps the caller
// isolated from writes a late stream goroutine may still perform.
func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return bytes.Clone(b.buf.Bytes())
}

// detach returns everything written so far without copying it. The returned
// slice aliases the buffer's storage, so it may only be used once no writer is
// left: see the branch in ExecCommandInPod that calls it. Callers that cannot
// prove that must use Bytes instead.
func (b *syncBuffer) detach() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Bytes()
}

// String returns everything written so far as a string.
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}
