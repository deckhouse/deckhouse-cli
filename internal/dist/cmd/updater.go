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

package distcmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/rpp"
	rppflags "github.com/deckhouse/deckhouse-cli/internal/rpp/flags"
	"github.com/deckhouse/deckhouse-cli/internal/selfupdate"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/deckhouse/deckhouse-cli/internal/version"
)

// activeVersionTag reports the running deckhouse-cli version. For a
// store-managed install the store's `current` symlink names the active version
// reliably even when the binary was built without version ldflags; trust it
// only when this invocation runs through the store.
func activeVersionTag(store *selfupdate.Store) string {
	current := version.Version

	if exePath, err := selfupdate.CurrentExecutable(); err == nil && store.Contains(exePath) {
		if tag := store.CurrentTag(); tag != "" {
			current = tag
		}
	}

	return current
}

// newUpdater builds an Updater backed by the registry-packages-proxy, reached
// with the kubeconfig identity from the command's flags.
func newUpdater(ctx context.Context, cmd *cobra.Command, logger *dkplog.Logger) (*selfupdate.Updater, error) {
	kubeconfig, _ := cmd.Flags().GetString("kubeconfig")
	kubeContext, _ := cmd.Flags().GetString("context")

	restConfig, kube, err := utilk8s.SetupK8sClientSet(kubeconfig, kubeContext,
		utilk8s.WithInsecureSkipTLSVerify(rppflags.InsecureSkipTLSVerify))
	if err != nil {
		return nil, fmt.Errorf("set up kubernetes client: %w", err)
	}

	client, err := rpp.NewClusterClient(
		ctx, kube, restConfig, logger.Named("registry-packages-proxy"),
		rppflags.Endpoint, rppflags.CAFile, rppflags.InsecureSkipTLSVerify,
	)
	if err != nil {
		return nil, fmt.Errorf("build registry-packages-proxy client: %w", err)
	}

	store, err := selfupdate.NewStore()
	if err != nil {
		// A nil store only disables retention for `d8 dist use`; updating still works.
		logger.Debug("version store unavailable", dkplog.Err(err))
	}

	return selfupdate.NewUpdater(selfupdate.NewRPPSource(client), store, logger.Named("selfupdate")), nil
}
