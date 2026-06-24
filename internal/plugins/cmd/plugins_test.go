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

package pluginscmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	rppflags "github.com/deckhouse/deckhouse-cli/internal/rpp/flags"
)

// minimalKubeconfig is just enough for SetupK8sClientSet to build a clientset; no
// connection is made (the command under test issues no proxy request).
const minimalKubeconfig = `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user:
    token: test-token
`

func TestPluginsDirFlagIsHonored(t *testing.T) {
	// The command object captures the plugins dir at REGISTRATION time; the
	// --plugins-dir flag is parsed later and must still take effect.
	//
	// Startup eagerly builds the registry-packages-proxy client, so point the
	// kubeconfig and endpoint at throwaway values: 'list' reads only the local
	// plugins dir and makes no request, so no cluster is needed.
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(minimalKubeconfig), 0o600))

	prevDir, prevKube, prevEndpoint := flags.DeckhousePluginsDir, flags.Kubeconfig, rppflags.Endpoint
	t.Cleanup(func() {
		flags.DeckhousePluginsDir, flags.Kubeconfig, rppflags.Endpoint = prevDir, prevKube, prevEndpoint
	})
	flags.Kubeconfig = kubeconfig
	rppflags.Endpoint = "https://127.0.0.1:4219"

	dir := t.TempDir() + "/custom-root"

	cmd := NewCommand(dkplog.NewNop())
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"list", "--plugins-dir", dir})
	cmd.SetOut(io.Discard)

	require.NoError(t, cmd.Execute())

	_, err := os.Stat(layout.PluginsRoot(dir))
	require.NoError(t, err, "the install root must be created under the --plugins-dir value, not the default")
}
