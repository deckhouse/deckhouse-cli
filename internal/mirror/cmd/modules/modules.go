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

package modules

import (
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/modules/pull"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/modules/push"
)

var mirrorModulesLong = templates.LongDesc(`
Copy Deckhouse modules images from ModuleSource to local filesystem and to third-party registry.
		
This command is used to manually upload Deckhouse modules images to an air-gapped registry.

For more information on how to use it, consult the docs at 
https://deckhouse.io/documentation/v1/deckhouse-faq.html#manually-uploading-images-of-deckhouse-modules-into-an-isolated-private-registry

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	mirrorModulesCmd := &cobra.Command{
		Use:           "modules",
		Short:         "Copy Deckhouse modules images from ModuleSource to local filesystem and to third-party registry",
		Long:          mirrorModulesLong,
		SilenceErrors: true,
	}

	mirrorModulesCmd.AddCommand(
		pull.NewCommand(),
		push.NewCommand(),
	)

	logs.AddFlags(mirrorModulesCmd.PersistentFlags())
	return mirrorModulesCmd
}
