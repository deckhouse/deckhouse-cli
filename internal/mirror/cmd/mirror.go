// Copyright 2024 Flant JSC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mirror

import (
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/push"
)

var (
	mirrorLong = templates.LongDesc(`
Copy Deckhouse Kubernetes Platform distribution to the local filesystem or 
the air-gapped registries.

For more information on how to use it, consult the docs at 
https://deckhouse.io/documentation/v1/deckhouse-faq.html#manually-uploading-images-to-an-air-gapped-registry

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)
)

func NewCommand() *cobra.Command {
	mirrorCmd := &cobra.Command{
		Use:   "mirror",
		Short: "Copy Deckhouse Kubernetes Platform distribution to the local filesystem or third-party registry",
		Long:  mirrorLong,
	}

	mirrorCmd.AddCommand(
		pull.NewCommand(),
		push.NewCommand(),
		modules.NewCommand(),
	)

	logs.AddFlags(mirrorCmd.PersistentFlags())
	return mirrorCmd
}
