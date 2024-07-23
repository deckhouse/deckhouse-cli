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

package mirror

import (
	"os"
	"strconv"

	"github.com/google/go-containerregistry/pkg/logs"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/push"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/vulndb"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/log"
)

var mirrorLong = templates.LongDesc(`
Copy Deckhouse Kubernetes Platform distribution to the local filesystem or 
the air-gapped registries.

For more information on how to use it, consult the docs at 
https://deckhouse.io/documentation/v1/deckhouse-faq.html#manually-uploading-images-to-an-air-gapped-registry

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

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
		vulndb.NewCommand(),
	)

	debugLogStr := os.Getenv("MIRROR_DEBUG_LOG")
	if debugLogStr != "" {
		debugLogLevel, err := strconv.Atoi(debugLogStr)
		if err != nil {
			log.WarnF("Invalid $MIRROR_DEBUG_LOG: %v\nUse 1 for progress logging, 2 for warnings or 3 for connection logging. Each level also enables previous ones.\n", err)
		}

		switch {
		case debugLogLevel >= 3:
			logs.Debug.SetOutput(os.Stderr)
		case debugLogLevel >= 2:
			logs.Warn.SetOutput(os.Stderr)
		case debugLogLevel >= 1:
			logs.Progress.SetOutput(os.Stderr)
		}
	}

	return mirrorCmd
}
