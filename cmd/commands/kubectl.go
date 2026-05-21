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

package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	kubecmd "k8s.io/kubectl/pkg/cmd"
	"k8s.io/kubectl/pkg/cmd/plugin"
)

const (
	cmNamespace = "d8-system"
	cmName      = "debug-container"
	cmImageKey  = "image"
)

var d8CommandRegex = regexp.MustCompile("([\"'`])d8 (\\w+)")

// d8KubectlWriter wraps an io.Writer and rewrites kubectl's "d8 <subcmd>"
// references (matched by d8CommandRegex) to "d8 k <subcmd>" on each Write call.
//
// kubectl uses os.Args[0] as the displayed command name in user-facing
// suggestions (e.g. "You can run `d8 replace -f ...` to try this update again.").
// Since the binary is invoked as "d8", those suggestions point users to a
// non-existent top-level command. Wrapping IOStreams.ErrOut with this writer
// ensures the suggestions are rewritten to the correct "d8 k <subcmd>" form
// before reaching the terminal.
type d8KubectlWriter struct {
	w io.Writer
}

func newD8KubectlWriter(w io.Writer) *d8KubectlWriter {
	return &d8KubectlWriter{w: w}
}

func (d *d8KubectlWriter) Write(p []byte) (int, error) {
	rewritten := d8CommandRegex.ReplaceAllString(string(p), "${1}d8 k ${2}")
	if _, err := d.w.Write([]byte(rewritten)); err != nil {
		return 0, err
	}
	// Report the original input length to honor the io.Writer contract even
	// though the rewritten payload may have a different byte length.
	return len(p), nil
}

// wrapRunE wraps all RunE functions in the kubectl command tree to intercept stderr output.
// This approach is preferred over modifying os.Args[0] because:
// - It avoids modifying global state (os.Args) which could affect other parts of the system
// - It provides surgical precision, only affecting kubectl error messages
// - It preserves the integrity of os.Args for logging, debugging, and other tools
// - It maintains clean separation of concerns without side effects
func wrapRunE(cmd *cobra.Command) {
	if cmd.RunE != nil {
		originalRunE := cmd.RunE

		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			// Create a pipe to capture stderr
			r, w, err := os.Pipe()
			if err != nil {
				return originalRunE(cmd, args)
			}

			oldStderr := os.Stderr
			os.Stderr = w

			err = originalRunE(cmd, args)
			w.Close()
			os.Stderr = oldStderr

			// Read the captured output
			output, readErr := io.ReadAll(r)
			r.Close()

			if readErr != nil {
				return err
			}

			// Modify the output to fix the command suggestion
			modifiedOutput := d8CommandRegex.ReplaceAllString(string(output), "${1}d8 k ${2}")

			// Write the modified output to real stderr
			fmt.Fprint(oldStderr, modifiedOutput)

			return err
		}
	}

	for _, sub := range cmd.Commands() {
		wrapRunE(sub)
	}
}

func getDebugImage(cmd *cobra.Command) (string, error) {
	configFlags := genericclioptions.NewConfigFlags(true)
	if val, err := cmd.InheritedFlags().GetString("kubeconfig"); err == nil {
		configFlags.KubeConfig = &val
	}
	if val, err := cmd.InheritedFlags().GetString("context"); err == nil {
		configFlags.Context = &val
	}

	restConfig, err := configFlags.ToRESTConfig()
	if err != nil {
		return "", fmt.Errorf("Failed to create Kubernetes client: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return "", fmt.Errorf("Failed to create Kubernetes client: %w", err)
	}

	var ErrGenericImageFetch = errors.New("Cannot get debug image from cluster, please specify --image explicitly")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	configMap, err := kubeCl.CoreV1().ConfigMaps(cmNamespace).Get(ctx, cmName, v1.GetOptions{})
	if err != nil {
		return "", ErrGenericImageFetch
	}

	imageName, ok := configMap.Data[cmImageKey]
	if !ok || imageName == "" {
		return "", ErrGenericImageFetch
	}

	return imageName, nil
}

func NewKubectlCommand() *cobra.Command {
	// Build a kubectl command tree with stderr wrapped by d8KubectlWriter so
	// kubectl's "d8 <subcmd>" command hints are rewritten to "d8 k <subcmd>".
	//
	// This must be applied at construction time: kubectl captures os.Stderr
	// once into IOStreams.ErrOut and stores the reference (see
	// k8s.io/kubectl/pkg/cmd/cmd.go:NewDefaultKubectlCommand). Code paths that
	// write via that stored reference (e.g. the post-edit hint
	// "You can run `d8 replace -f ...`" emitted from editoptions.go) cannot be
	// intercepted by later swaps of the os.Stderr global.
	ioStreams := genericiooptions.IOStreams{
		In:     os.Stdin,
		Out:    os.Stdout,
		ErrOut: newD8KubectlWriter(os.Stderr),
	}

	kubectlCmd := kubecmd.NewDefaultKubectlCommandWithArgs(kubecmd.KubectlOptions{
		PluginHandler: kubecmd.NewDefaultPluginHandler(plugin.ValidPluginFilenamePrefixes),
		Arguments:     os.Args,
		ConfigFlags: genericclioptions.NewConfigFlags(true).
			WithDeprecatedPasswordFlag().
			WithDiscoveryBurst(300).
			WithDiscoveryQPS(50.0).
			WithWarningPrinter(ioStreams),
		IOStreams: ioStreams,
	})
	kubectlCmd.Use = "k"
	kubectlCmd.Aliases = []string{"kubectl"}
	kubectlCmd = ReplaceCommandName("kubectl", "d8 k", kubectlCmd)

	// Fallback rewrite for kubectl code paths that write to the global
	// os.Stderr directly instead of using IOStreams.ErrOut.
	wrapRunE(kubectlCmd)

	var debugCmd *cobra.Command
	for _, cmd := range kubectlCmd.Commands() {
		if cmd.Name() == "debug" {
			debugCmd = cmd
			break
		}
	}

	if debugCmd != nil {
		if imageFlag := debugCmd.Flags().Lookup("image"); imageFlag != nil {
			imageFlag.Usage = "Container image to use for debug container. If not specified, the platform's recommended image will be used."
		}
	}

	originalPersistentPreRunE := kubectlCmd.PersistentPreRunE
	kubectlCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Restore default OS signal handling for the kubectl subtree.
		//
		// The d8 root command installs a graceful-termination signal handler
		// (see graceful.WithTermination in NewDeliveryCommand) that intercepts
		// SIGINT/SIGTERM, cancels the root context and then resets the signal
		// handlers. The kubectl subcommands (notably long-running ones such as
		// `proxy`, `port-forward`, `exec`, `attach`, `logs -f`) do not observe
		// that context and keep running until a second signal arrives and is
		// delivered to the default Go handler.
		//
		// To match standalone kubectl behavior (single SIGINT/SIGTERM stops the
		// command), drop our signal interceptor before kubectl starts so the
		// very first signal is delivered to the default handler and terminates
		// the process immediately.
		signal.Reset(syscall.SIGINT, syscall.SIGTERM)

		if cmd.Name() == "debug" || (cmd.Parent() != nil && cmd.Parent().Name() == "debug") {
			imageFlag := cmd.Flags().Lookup("image")
			if imageFlag != nil && imageFlag.Value.String() == "" {
				debugImage, err := getDebugImage(cmd)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Warning: cannot get debug container image from cluster: %v\n", err)
					fmt.Fprintf(os.Stderr, "Continuing with default kubectl behavior...\n")
				} else {
					fmt.Fprintf(os.Stderr, "Using debug container image: %s\n", debugImage)
					if err := cmd.Flags().Set("image", debugImage); err != nil {
						fmt.Fprintf(os.Stderr, "Warning: cannot set debug image flag: %v\n", err)
					}
				}
			}
		}

		if originalPersistentPreRunE != nil {
			return originalPersistentPreRunE(cmd, args)
		}
		panic("originalPersistentPreRunE is nil, cannot proceed")
	}

	// Based on https://github.com/kubernetes/kubernetes/blob/v1.29.3/staging/src/k8s.io/component-base/cli/run.go#L88

	kubectlCmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)
	kubectlCmd.SilenceErrors = true
	logs.AddFlags(kubectlCmd.PersistentFlags())

	switch {
	case kubectlCmd.PersistentPreRun != nil:
		pre := kubectlCmd.PersistentPreRun
		kubectlCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
			pre(cmd, args)
		}
	case kubectlCmd.PersistentPreRunE != nil:
		pre := kubectlCmd.PersistentPreRunE
		kubectlCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			logs.InitLogs()
			return pre(cmd, args)
		}
	default:
		kubectlCmd.PersistentPreRun = func(_ *cobra.Command, _ []string) {
			logs.InitLogs()
		}
	}

	return kubectlCmd
}
