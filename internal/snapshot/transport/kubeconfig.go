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

package transport

import (
	"fmt"
	"path/filepath"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// KubeconfigNamespace resolves the namespace selected by the given kubeconfig
// path and context, mirroring kubectl: it returns the context's namespace, or
// "default" when the context does not pin one. It uses the same loading rules
// as utilk8s.SetupK8sClientSet so flag handling stays consistent across commands.
func KubeconfigNamespace(kubeconfigPath, contextName string) (string, error) {
	namespace, _, err := newClientConfig(kubeconfigPath, contextName).Namespace()
	if err != nil {
		return "", fmt.Errorf("resolving namespace from kubeconfig: %w", err)
	}

	return namespace, nil
}

// newClientConfig mirrors utilk8s.SetupK8sClientSet's loading rules so
// namespace resolution stays consistent with the REST config snapshot
// commands build from the same kubeconfig/context flags.
func newClientConfig(kubeconfigPath, contextName string) clientcmd.ClientConfig {
	var configOverrides *clientcmd.ConfigOverrides
	if contextName != utilk8s.DefaultKubeContext {
		configOverrides = &clientcmd.ConfigOverrides{
			CurrentContext: contextName,
		}
	}

	kubeconfigFiles := filepath.SplitList(kubeconfigPath)
	chain := make([]string, 0, len(kubeconfigFiles))
	loadingRules := &clientcmd.ClientConfigLoadingRules{}

	chain = append(chain, deduplicate(kubeconfigFiles)...)

	if len(chain) > 1 {
		loadingRules.Precedence = kubeconfigFiles
		loadingRules.WarnIfAllMissing = true
	} else {
		loadingRules.ExplicitPath = kubeconfigPath
	}

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
}

// deduplicate removes any duplicated values and returns a new slice, keeping the order unchanged.
func deduplicate(s []string) []string {
	encountered := map[string]bool{}
	ret := make([]string, 0)

	for i := range s {
		if encountered[s[i]] {
			continue
		}

		encountered[s[i]] = true
		ret = append(ret, s[i])
	}

	return ret
}
