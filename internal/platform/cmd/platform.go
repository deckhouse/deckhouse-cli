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

package platform

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

        "github.com/deckhouse/deckhouse-cli/internal/platform/cmd/edit"
//	cluster_config "github.com/deckhouse/deckhouse-cli/internal/edit/cmd/cluster-configuration"
//	provider_config "github.com/deckhouse/deckhouse-cli/internal/edit/cmd/provider-cluster-configuration"
//	static_config "github.com/deckhouse/deckhouse-cli/internal/edit/cmd/static-cluster-configuration"
	"github.com/deckhouse/deckhouse-cli/internal/platform/flags"
)

var platformLong = templates.LongDesc(`
Change configuration files in Kubernetes cluster conveniently and safely.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	platformCmd := &cobra.Command{
		Use: "platform", Short: "Edit configuration files",
		Long:    platformLong,
		PreRunE: flags.ValidateParameters,
	}

	platformCmd.AddCommand(
		edit.NewCommand(),
		//cluster_config.NewCommand(),
		//static_config.NewCommand(),
		//provider_config.NewCommand(),
	)

	flags.AddPersistentFlags(platformCmd)

	return platformCmd
}
