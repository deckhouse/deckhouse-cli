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

package client

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

const kubeconfigCluster = `
clusters:
- name: c
  cluster:
    server: https://127.0.0.1:6443
users:
- name: u
  user:
    token: t
`

func writeKubeconfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

func TestSafeClientNamespace(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  string
	}{
		{
			name: "namespace of the current context",
			files: []string{`
current-context: a
contexts:
- name: a
  context: {cluster: c, user: u, namespace: team-a}
- name: b
  context: {cluster: c, user: u, namespace: team-b}
` + kubeconfigCluster},
			want: "team-a",
		},
		{
			name: "current context without a namespace",
			files: []string{`
current-context: a
contexts:
- name: a
  context: {cluster: c, user: u}
` + kubeconfigCluster},
			want: "default",
		},
		{
			name: "KUBECONFIG of several files",
			files: []string{
				`
current-context: b
`,
				`
current-context: a
contexts:
- name: a
  context: {cluster: c, user: u, namespace: team-a}
- name: b
  context: {cluster: c, user: u, namespace: team-b}
` + kubeconfigCluster,
			},
			want: "team-b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paths := make([]string, 0, len(tt.files))
			for _, content := range tt.files {
				paths = append(paths, writeKubeconfig(t, content))
			}

			t.Setenv("KUBECONFIG", strings.Join(paths, string(os.PathListSeparator)))

			sc, err := NewSafeClient(pflag.NewFlagSet("test", pflag.ContinueOnError))
			require.NoError(t, err)

			got, err := sc.Namespace()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)

			got, err = sc.Copy().Namespace()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSafeClientNamespaceWithoutKubeconfig(t *testing.T) {
	_, err := NewSafeClientForConfig(&rest.Config{Host: "https://127.0.0.1:6443"}).Namespace()
	require.Error(t, err)
}
