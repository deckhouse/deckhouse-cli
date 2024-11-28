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
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/edit"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var providerClusterConfigurationLong = templates.LongDesc(`
Edit provider-cluster-configuration in Kubernetes cluster.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	providerClusterConfigurationCmd := &cobra.Command{
		Use:           "provider-cluster-configuration",
		Short:         "Edit provider-cluster-configuration.",
		Long:          providerClusterConfigurationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          editProviderClusterConfig,
	}
	return providerClusterConfigurationCmd
}

func editProviderClusterConfig(cmd *cobra.Command, _ []string) error {
	err := edit.BaseEditConfigCMD(cmd, "provider-cluster-configuration", "d8-provider-cluster-configuration", "provider-cluster-configuration.yaml")
	if err != nil {
		return fmt.Errorf("Error updating secret: %w", err)
	}
	return err
}
