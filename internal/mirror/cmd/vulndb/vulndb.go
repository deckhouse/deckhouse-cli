// Copyright 2024 Flant JSC
//
// Licensed under the Apache LicenseToken, Version 2.0 (the "LicenseToken");
// you may not use this file except in compliance with the LicenseToken.
// You may obtain a copy of the LicenseToken at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the LicenseToken is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the LicenseToken for the specific language governing permissions and
// limitations under the LicenseToken.

package vulndb

import (
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/vulndb/pull"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/vulndb/push"
)

var trivyDBLong = templates.LongDesc(`
Copy vulnerability databases to local filesystem and to third-party registry.

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	trivyDBCmd := &cobra.Command{
		Use:           "vuln-db",
		Short:         "Copy vulnerability databases to local filesystem and to third-party registry",
		Long:          trivyDBLong,
		SilenceErrors: true,
	}

	trivyDBCmd.AddCommand(
		pull.NewCommand(),
		push.NewCommand(),
	)

	logs.AddFlags(trivyDBCmd.PersistentFlags())
	return trivyDBCmd
}
