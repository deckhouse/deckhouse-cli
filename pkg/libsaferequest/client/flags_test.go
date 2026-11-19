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
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

const (
	envKubeconfig = `
current-context: env
contexts:
- name: env
  context: {cluster: env, user: u, namespace: team-env}
clusters:
- name: env
  cluster:
    server: https://env.example:6443
users:
- name: u
  user:
    token: t
`
	flagKubeconfig = `
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
)

func kubeconfigFlagSet(t *testing.T, defaultKubeconfig string, args ...string) *pflag.FlagSet {
	t.Helper()

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.StringP(kubeconfigFlag, "k", defaultKubeconfig, "")
	flags.String(contextFlag, "", "")
	require.NoError(t, flags.Parse(args))

	return flags
}

func TestNewSafeClientKubeconfigFlags(t *testing.T) {
	envFile := writeKubeconfig(t, envKubeconfig)
	flagFile := writeKubeconfig(t, flagKubeconfig)
	currentOther := writeKubeconfig(t, "current-context: other\n")
	list := currentOther + string(os.PathListSeparator) + flagFile

	tests := []struct {
		name              string
		kubeconfigEnv     string
		defaultKubeconfig string
		args              []string
		wantNS            string
		wantHost          string
	}{
		{"KUBECONFIG without flags", envFile, "", nil, "team-env", "https://env.example:6443"},
		{"--kubeconfig left at its default", envFile, flagFile, nil, "team-env", "https://env.example:6443"},
		{"--kubeconfig", envFile, "", []string{"--kubeconfig", flagFile}, "team-main", "https://main.example:6443"},
		{"-k", envFile, "", []string{"-k", flagFile}, "team-main", "https://main.example:6443"},
		{"--kubeconfig and --context", envFile, "", []string{"--kubeconfig", flagFile, "--context", "other"}, "team-other", "https://other.example:6443"},
		{"--context with KUBECONFIG", flagFile, "", []string{"--context", "other"}, "team-other", "https://other.example:6443"},
		{"--kubeconfig with several files", envFile, "", []string{"--kubeconfig", list}, "team-other", "https://other.example:6443"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KUBECONFIG", tt.kubeconfigEnv)

			sc, err := NewSafeClient(nil, kubeconfigFlagSet(t, tt.defaultKubeconfig, tt.args...))
			require.NoError(t, err)

			require.Equal(t, tt.wantHost, sc.RESTConfig().Host)

			namespace, err := sc.Namespace()
			require.NoError(t, err)
			require.Equal(t, tt.wantNS, namespace)
		})
	}
}

func TestNewSafeClientMissingKubeconfigFlagFile(t *testing.T) {
	t.Setenv("KUBECONFIG", writeKubeconfig(t, envKubeconfig))

	_, err := NewSafeClient(kubeconfigFlagSet(t, "", "--kubeconfig", "/nonexistent/kubeconfig"))
	require.Error(t, err)
}
