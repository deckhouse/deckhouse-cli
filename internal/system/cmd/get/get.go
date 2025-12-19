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

package get

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	cluster_config "github.com/deckhouse/deckhouse-cli/internal/system/cmd/get/cluster-configuration"
	providerconfig "github.com/deckhouse/deckhouse-cli/internal/system/cmd/get/provider-cluster-configuration"
	static_config "github.com/deckhouse/deckhouse-cli/internal/system/cmd/get/static-cluster-configuration"
)

var getLong = templates.LongDesc(`
Get configuration files from Kubernetes cluster.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	getCmd := &cobra.Command{
		Use: "get", Short: "Get configuration files",
		Long: getLong,
	}

	getCmd.AddCommand(
		cluster_config.NewCommand(),
		static_config.NewCommand(),
		providerconfig.NewCommand(),
	)

	return getCmd
}
