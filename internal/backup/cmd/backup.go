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

package backup

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	cluster_config "github.com/deckhouse/deckhouse-cli/internal/backup/cmd/cluster-config"
	"github.com/deckhouse/deckhouse-cli/internal/backup/cmd/etcd"
	"github.com/deckhouse/deckhouse-cli/internal/backup/cmd/loki"
)

var backupLong = templates.LongDesc(`
Backup various parts of Deckhouse Kubernetes Platform

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup various parts of Deckhouse Kubernetes Platform",
		Long:  backupLong,
	}

	addPersistentFlags(backupCmd.PersistentFlags())

	backupCmd.AddCommand(
		etcd.NewCommand(),
		cluster_config.NewCommand(),
		loki.NewCommand(),
	)

	return backupCmd
}
