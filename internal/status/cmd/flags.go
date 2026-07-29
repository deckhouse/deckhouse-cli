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
	"github.com/spf13/pflag"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func addPersistentFlags(flagSet *pflag.FlagSet) {
	defaultKubeconfigPath := utilk8s.DefaultKubeconfigPath()

	flagSet.StringP(
		"kubeconfig", "k",
		defaultKubeconfigPath,
		"Path to kubeconfig file. (default is $KUBECONFIG when it is set, otherwise the default kubeconfig path for the current OS user)",
	)

	flagSet.String("context", "", "The name of the kubeconfig context to use")
}
