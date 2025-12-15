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

package cmd

import (
	"os"

	"github.com/spf13/pflag"
)

func addFlags(flags *pflag.FlagSet) {
	flags.Bool(
		"retry",
		false,
		"Retry annotation for previously failed objects.",
	)

	flags.String(
		"as",
		"system:serviceaccount:d8-system:deckhouse",
		"Specify a Kubernetes service account for the kubectl operations (impersonation).",
	)

	flags.String(
		"log-level",
		"DEBUG",
		"Set the log level (INFO, DEBUG, TRACE). Defaults to DEBUG.",
	)

	defaultKubeconfigPath := os.ExpandEnv("$HOME/.kube/config")
	if p := os.Getenv("KUBECONFIG"); p != "" {
		defaultKubeconfigPath = p
	}

	flags.String(
		"kubeconfig",
		defaultKubeconfigPath,
		"Path to the kubeconfig file to use for CLI requests. (default is $KUBECONFIG when it is set, $HOME/.kube/config otherwise)",
	)

	flags.String(
		"context",
		"kubernetes-admin@kubernetes",
		"The name of the kubeconfig context to use.",
	)
}
