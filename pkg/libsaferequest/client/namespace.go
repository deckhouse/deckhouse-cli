/*
Copyright 2026 Flant JSC

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

package client

import "k8s.io/client-go/tools/clientcmd"

// DefaultNamespace returns the namespace from the current kubeconfig context.
// If the context has no namespace set, or if the kubeconfig cannot be loaded,
// it falls back to "default" — matching kubectl's behaviour.
func DefaultNamespace() string {
	ns, _, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	).Namespace()
	if err != nil || ns == "" {
		return "default"
	}

	return ns
}
