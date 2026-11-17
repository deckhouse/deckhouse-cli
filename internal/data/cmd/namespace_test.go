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

package cmd

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
	exportutil "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	importutil "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const kubeconfigWithNamespace = `
apiVersion: v1
kind: Config
current-context: work
contexts:
- name: work
  context: {cluster: c, user: u, namespace: team-work}
clusters:
- name: c
  cluster:
    server: https://127.0.0.1:6443
users:
- name: u
  user:
    token: t
`

var errResolved = errors.New("namespace resolved")

func stubResolveClient(t *testing.T) *string {
	t.Helper()

	var got string

	resolve := func(_ context.Context, _ *safeClient.SafeClient, namespace string, _ *slog.Logger) (dataapi.Backend, ctrlrtclient.Client, error) {
		got = namespace

		return dataapi.Backend{}, nil, errResolved
	}

	oldExport, oldImport, oldNoAuth := exportutil.ResolveClientFunc, importutil.ResolveClientFunc, safeClient.SupportNoAuth
	exportutil.ResolveClientFunc, importutil.ResolveClientFunc = resolve, resolve

	t.Cleanup(func() {
		exportutil.ResolveClientFunc, importutil.ResolveClientFunc, safeClient.SupportNoAuth = oldExport, oldImport, oldNoAuth
	})

	return &got
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

func TestSubcommandsTakeNamespaceFromKubeconfig(t *testing.T) {
	t.Setenv("KUBECONFIG", writeFile(t, "config", kubeconfigWithNamespace))

	pvc := writeFile(t, "pvc.yaml", "spec: {}\n")
	pvcWithNamespace := writeFile(t, "pvc-ns.yaml", "metadata: {namespace: team-manifest}\nspec: {}\n")

	tests := []struct {
		name string
		args []string
		want string
	}{
		{"export create", []string{"export", "create", "e", "pvc/p"}, "team-work"},
		{"export create -n", []string{"export", "create", "-n", "team-flag", "e", "pvc/p"}, "team-flag"},
		{"export delete", []string{"export", "delete", "e"}, "team-work"},
		{"export delete -n", []string{"export", "delete", "-n", "team-flag", "e"}, "team-flag"},
		{"export download", []string{"export", "download", "e", "/f"}, "team-work"},
		{"export download -n", []string{"export", "download", "--namespace", "team-flag", "e", "/f"}, "team-flag"},
		{"export list", []string{"export", "list", "e"}, "team-work"},
		{"export list -n", []string{"export", "list", "-n", "team-flag", "e"}, "team-flag"},
		{"import create", []string{"import", "create", "i", "-f", pvc}, "team-work"},
		{"import create manifest namespace", []string{"import", "create", "i", "-f", pvcWithNamespace}, "team-manifest"},
		{"import create -n", []string{"import", "create", "-n", "team-flag", "i", "-f", pvcWithNamespace}, "team-flag"},
		{"import delete", []string{"import", "delete", "i"}, "team-work"},
		{"import delete -n", []string{"import", "delete", "-n", "team-flag", "i"}, "team-flag"},
		{"import upload", []string{"import", "upload", "i"}, "team-work"},
		{"import upload -n", []string{"import", "upload", "-n", "team-flag", "i"}, "team-flag"},
		{"deprecated create", []string{"create", "e", "pvc/p"}, "team-work"},
		{"deprecated create -n", []string{"create", "-n", "team-flag", "e", "pvc/p"}, "team-flag"},
		{"deprecated delete", []string{"delete", "e"}, "team-work"},
		{"deprecated download", []string{"download", "e", "/f"}, "team-work"},
		{"deprecated list", []string{"list", "e"}, "team-work"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stubResolveClient(t)

			root := NewCommand()
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs(tt.args)

			require.ErrorIs(t, root.Execute(), errResolved)
			require.Equal(t, tt.want, *got)
		})
	}
}
