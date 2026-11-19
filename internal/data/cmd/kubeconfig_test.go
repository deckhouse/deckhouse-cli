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
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
	exportutil "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	importutil "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const kubeconfigWithTwoContexts = `
apiVersion: v1
kind: Config
current-context: main
contexts:
- name: main
  context: {cluster: main, user: u, namespace: team-main}
- name: other
  context: {cluster: other, user: u, namespace: team-other}
clusters:
- name: main
  cluster:
    server: https://main.example:6443
- name: other
  cluster:
    server: https://other.example:6443
users:
- name: u
  user:
    token: t
`

type resolvedTarget struct {
	namespace string
	host      string
}

func stubResolveClientTarget(t *testing.T) *resolvedTarget {
	t.Helper()

	got := &resolvedTarget{}

	resolve := func(_ context.Context, sc *safeClient.SafeClient, namespace string, _ *slog.Logger) (dataapi.Backend, ctrlrtclient.Client, error) {
		got.namespace, got.host = namespace, sc.RESTConfig().Host

		return dataapi.Backend{}, nil, errResolved
	}

	oldExport, oldImport, oldNoAuth := exportutil.ResolveClientFunc, importutil.ResolveClientFunc, safeClient.SupportNoAuth
	exportutil.ResolveClientFunc, importutil.ResolveClientFunc = resolve, resolve

	t.Cleanup(func() {
		exportutil.ResolveClientFunc, importutil.ResolveClientFunc, safeClient.SupportNoAuth = oldExport, oldImport, oldNoAuth
	})

	return got
}

func TestSubcommandsTakeKubeconfigAndContextFlags(t *testing.T) {
	t.Setenv("KUBECONFIG", writeFile(t, "env-config", kubeconfigWithNamespace))

	kubeconfig := writeFile(t, "config", kubeconfigWithTwoContexts)
	pvc := writeFile(t, "pvc.yaml", "spec: {}\n")

	other := resolvedTarget{namespace: "team-other", host: "https://other.example:6443"}
	current := resolvedTarget{namespace: "team-main", host: "https://main.example:6443"}

	tests := []struct {
		name string
		args []string
		want resolvedTarget
	}{
		{"export create", []string{"export", "create", "--kubeconfig", kubeconfig, "--context", "other", "e", "pvc/p"}, other},
		{"export delete", []string{"export", "delete", "--kubeconfig", kubeconfig, "--context", "other", "e"}, other},
		{"export download", []string{"export", "download", "--kubeconfig", kubeconfig, "--context", "other", "e", "/f"}, other},
		{"export list", []string{"export", "list", "--kubeconfig", kubeconfig, "--context", "other", "e"}, other},
		{"import create", []string{"import", "create", "--kubeconfig", kubeconfig, "--context", "other", "i", "-f", pvc}, other},
		{"import delete", []string{"import", "delete", "--kubeconfig", kubeconfig, "--context", "other", "i"}, other},
		{"import upload", []string{"import", "upload", "--kubeconfig", kubeconfig, "--context", "other", "i"}, other},
		{"deprecated create", []string{"create", "--kubeconfig", kubeconfig, "--context", "other", "e", "pvc/p"}, other},
		{"deprecated delete", []string{"delete", "--kubeconfig", kubeconfig, "--context", "other", "e"}, other},
		{"deprecated download", []string{"download", "--kubeconfig", kubeconfig, "--context", "other", "e", "/f"}, other},
		{"deprecated list", []string{"list", "--kubeconfig", kubeconfig, "--context", "other", "e"}, other},
		{"-k before the subcommand", []string{"-k", kubeconfig, "export", "create", "e", "pvc/p"}, current},
		{"-n wins over the context", []string{"export", "create", "-k", kubeconfig, "--context", "other", "-n", "team-flag", "e", "pvc/p"},
			resolvedTarget{namespace: "team-flag", host: other.host}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stubResolveClientTarget(t)

			root := NewCommand()
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs(tt.args)

			require.ErrorIs(t, root.Execute(), errResolved)
			require.Equal(t, tt.want, *got)
		})
	}
}

func TestImportCreateReadsManifestFromStdin(t *testing.T) {
	t.Setenv("KUBECONFIG", writeFile(t, "config", kubeconfigWithNamespace))

	tests := []struct {
		name     string
		manifest string
		want     string
	}{
		{"manifest namespace", "metadata: {name: p, namespace: team-stdin}\nspec: {}\n", "team-stdin"},
		{"manifest without namespace", "metadata: {name: p}\nspec: {}\n", "team-work"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stubResolveClientTarget(t)

			root := NewCommand()
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetIn(strings.NewReader(tt.manifest))
			root.SetArgs([]string{"import", "create", "i", "-f", "-"})

			require.ErrorIs(t, root.Execute(), errResolved)
			require.Equal(t, tt.want, got.namespace)
		})
	}
}
