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

package status

import (
	"os"

	"github.com/spf13/pflag"
)

func addPersistentFlags(flagSet *pflag.FlagSet) {
	defaultKubeconfigPath := os.ExpandEnv("$HOME/.kube/config")
	if p := os.Getenv("KUBECONFIG"); p != "" {
		defaultKubeconfigPath = p
	}

	flagSet.StringP(
		"kubeconfig", "k",
		defaultKubeconfigPath,
		"KubeConfig of the cluster. (default is $KUBECONFIG when it is set, $HOME/.kube/config otherwise)",
	)

	flagSet.String("context", "", "The name of the kubeconfig context to use")
}
