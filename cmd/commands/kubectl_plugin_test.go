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

package commands

import (
	"slices"
	"testing"

	"k8s.io/cli-runtime/pkg/genericiooptions"
	kubecmd "k8s.io/kubectl/pkg/cmd"
)

func TestKubectlPluginArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"k drops the command word", []string{"d8", "k", "argo", "rollouts", "get"}, []string{"d8", "argo", "rollouts", "get"}},
		{"kubectl alias drops the command word", []string{"d8", "kubectl", "hello"}, []string{"d8", "hello"}},
		{"bare k", []string{"d8", "k"}, []string{"d8"}},
		{"other d8 command disables lookup", []string{"d8", "system", "queue", "list"}, []string{"d8"}},
		{"bare d8", []string{"d8"}, []string{"d8"}},
		{"empty argv", []string{}, []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := kubectlPluginArgs(tt.args); !slices.Equal(got, tt.want) {
				t.Errorf("kubectlPluginArgs(%q) = %q, want %q", tt.args, got, tt.want)
			}
		})
	}
}

// fakePluginHandler serves the plugins listed in installed and records lookups
// and the executed plugin instead of exec'ing a binary.
type fakePluginHandler struct {
	installed []string
	lookedUp  []string
	executed  string
	execArgs  []string
}

func (h *fakePluginHandler) Lookup(filename string) (string, bool) {
	h.lookedUp = append(h.lookedUp, filename)
	if slices.Contains(h.installed, filename) {
		return "/usr/local/bin/kubectl-" + filename, true
	}

	return "", false
}

func (h *fakePluginHandler) Execute(path string, args, _ []string) error {
	h.executed, h.execArgs = path, args
	return nil
}

func TestKubectlPluginLookup(t *testing.T) {
	tests := []struct {
		name         string
		argv         []string
		wantExecuted string
		wantArgs     []string
	}{
		{
			name:         "d8 k runs a multi-word plugin",
			argv:         []string{"d8", "k", "argo", "rollouts", "get", "rollout", "demo"},
			wantExecuted: "/usr/local/bin/kubectl-argo-rollouts",
			wantArgs:     []string{"get", "rollout", "demo"},
		},
		{
			name:         "d8 kubectl runs a plugin",
			argv:         []string{"d8", "kubectl", "argo", "rollouts", "version"},
			wantExecuted: "/usr/local/bin/kubectl-argo-rollouts",
			wantArgs:     []string{"version"},
		},
		{
			name: "other d8 command never runs a kubectl plugin",
			argv: []string{"d8", "argo", "rollouts"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &fakePluginHandler{installed: []string{"argo-rollouts"}}

			kubecmd.NewDefaultKubectlCommandWithArgs(kubecmd.KubectlOptions{
				PluginHandler: h,
				Arguments:     kubectlPluginArgs(tt.argv),
				IOStreams:     genericiooptions.NewTestIOStreamsDiscard(),
			})

			if h.executed != tt.wantExecuted || !slices.Equal(h.execArgs, tt.wantArgs) {
				t.Errorf("executed %q with %q, want %q with %q (lookups: %q)",
					h.executed, h.execArgs, tt.wantExecuted, tt.wantArgs, h.lookedUp)
			}
		})
	}
}
