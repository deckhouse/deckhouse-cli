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

package provider_config

import (
	edit "github.com/deckhouse/deckhouse-cli/internal/edit/cmd"
	"github.com/deckhouse/deckhouse-cli/internal/edit/utilk8s"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
	"log"
)

// TODO texts
var providerClusterConfigurationLong = templates.LongDesc(`
Edit Deckhouse Kubernetes Platform provider cluster configuration.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	providerClusterConfigurationCmd := &cobra.Command{
		Use:           "provider-cluster-configuration",
		Short:         "Edit Deckhouse Kubernetes Platform provider cluster configuration",
		Long:          providerClusterConfigurationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       edit.ValidateParameters,
		RunE:          editProviderClusterConfig,
	}

	edit.AddFlags(providerClusterConfigurationCmd.Flags())
	return providerClusterConfigurationCmd
}

func editProviderClusterConfig(cmd *cobra.Command, _ []string) error {
	err := utilk8s.BaseEditConfigCMD(cmd, "provider-cluster-configuration", "d8-provider-cluster-configuration", "provider-cluster-configuration.yaml")
	if err != nil {
		log.Fatalf("Error updating secret: %s", err.Error())
	}
	return err
}
