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
	"fmt"
	"os"

	"github.com/spf13/pflag"
)

func addFlags(flagSet *pflag.FlagSet) {
	defaultKubeconfigPath := os.ExpandEnv("$HOME/.kube/config")
	if p := os.Getenv("KUBECONFIG"); p != "" {
		defaultKubeconfigPath = p
	}

	flagSet.StringVarP(
		&kubeconfigPath,
		"kubeconfig", "k",
		defaultKubeconfigPath,
		"KubeConfig of the cluster. (default is $KUBECONFIG when it is set, $HOME/.kube/config otherwise)",
	)
	flagSet.StringVarP(
		&requestedEtcdPodName,
		"etcd-pod", "p",
		"",
		"Name of the etcd pod to snapshot from. (optional)",
	)
	flagSet.BoolVar(
		&verboseLog,
		"verbose",
		false,
		"Verbose log output.",
	)
}

func validateFlags() error {
	stats, err := os.Stat(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Invalid --kubeconfig: %w", err)
	}
	if !stats.Mode().IsRegular() {
		return fmt.Errorf("Invalid --kubeconfig: %s is not a regular file", kubeconfigPath)
	}

	return nil
}
