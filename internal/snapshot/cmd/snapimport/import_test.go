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

package snapimportcmd

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/snapimport"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
)

const commandSelectedHost = "https://selected.example.test"

func TestNewCommand_ParsesKubeconfigAndContextBeforeRun(t *testing.T) {
	helpCmd := NewCommand(slog.Default())

	var help bytes.Buffer

	helpCmd.SetOut(&help)
	helpCmd.SetErr(&help)
	helpCmd.SetArgs([]string{"--help"})
	if err := helpCmd.Execute(); err != nil {
		t.Fatalf("execute help: %v", err)
	}

	for _, flag := range []string{"-k, --kubeconfig", "--context"} {
		if !strings.Contains(help.String(), flag) {
			t.Fatalf("help does not contain %q:\n%s", flag, help.String())
		}
	}

	kubeconfigPath := writeCommandKubeconfig(t)
	stopAfterConfig := errors.New("stop after REST config")
	originalLoader := commandRESTConfigLoader
	t.Cleanup(func() {
		commandRESTConfigLoader = originalLoader
	})

	var gotHost string

	commandRESTConfigLoader = func(flagSets ...*pflag.FlagSet) (*rest.Config, error) {
		config, err := transport.NewRESTConfig(flagSets...)
		if err != nil {
			return nil, err
		}

		gotHost = config.Host

		return nil, stopAfterConfig
	}

	cmd := NewCommand(slog.Default())
	cmd.SetArgs([]string{
		"--namespace", "snapshot-ns",
		"--input", t.TempDir(),
		"--kubeconfig", kubeconfigPath,
		"--context", "selected",
	})

	err := cmd.Execute()
	if !errors.Is(err, stopAfterConfig) {
		t.Fatalf("execute error = %v, want stop hook", err)
	}

	if gotHost != commandSelectedHost {
		t.Fatalf("REST config host = %q, want %q", gotHost, commandSelectedHost)
	}

	namespace, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		t.Fatalf("read namespace flag: %v", err)
	}

	if namespace != "snapshot-ns" {
		t.Fatalf("snapshot namespace = %q, want %q", namespace, "snapshot-ns")
	}
}

func writeCommandKubeconfig(t *testing.T) string {
	t.Helper()

	kubeconfigPath := filepath.Join(t.TempDir(), "config")
	kubeconfig := []byte(`apiVersion: v1
kind: Config
clusters:
- name: default
  cluster:
    server: https://default.example.test
- name: selected
  cluster:
    server: ` + commandSelectedHost + `
contexts:
- name: default
  context:
    cluster: default
- name: selected
  context:
    cluster: selected
current-context: default
`)
	if err := os.WriteFile(kubeconfigPath, kubeconfig, 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	return kubeconfigPath
}

func TestNewCommand_Defaults(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	wantUse := cmdUse + " [flags]"
	if cmd.Use != wantUse {
		t.Fatalf("unexpected Use: got %q, want %q", cmd.Use, wantUse)
	}

	if !cmd.SilenceUsage {
		t.Fatal("SilenceUsage should be true")
	}

	if !cmd.SilenceErrors {
		t.Fatal("SilenceErrors should be true")
	}

	ttl, err := cmd.Flags().GetString(flagTTL)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagTTL, err)
	}

	if ttl != defaultImportTTL {
		t.Fatalf("default --%s: got %q, want %q", flagTTL, ttl, defaultImportTTL)
	}

	timeout, err := cmd.Flags().GetDuration(flagTimeout)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagTimeout, err)
	}

	if timeout != 20*time.Minute {
		t.Fatalf("default --%s: got %s, want 20m", flagTimeout, timeout)
	}

	skipUnsupported, err := cmd.Flags().GetBool(flagSkipUnsupportedFSEntries)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagSkipUnsupportedFSEntries, err)
	}

	if skipUnsupported {
		t.Fatalf("default --%s = true, want false", flagSkipUnsupportedFSEntries)
	}
}

func TestNewCommand_DocumentsFilesystemEntryLimitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fragment string
	}{
		{name: "regular file protocol", fragment: "protocol supports only regular-file"},
		{name: "links", fragment: "Symlink, hardlink"},
		{name: "special entries", fragment: "device, FIFO, and\n    socket entries"},
		{name: "empty directories", fragment: "empty directories other than well-known filesystem-reserved names"},
		{name: "directory side effect", fragment: "creates directories only as a side effect of a file PUT inside them"},
		{name: "lossy opt-in flag", fragment: "--skip-unsupported-fs-entries"},
		{name: "data loss warning", fragment: "causes data loss for each skipped path"},
		{name: "post-upload summary", fragment: "bounded post-upload summary"},
	}

	commandLong := NewCommand(slog.Default()).Long
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if !strings.Contains(commandLong, tc.fragment) {
				t.Errorf("command Long does not contain %q:\n%s", tc.fragment, commandLong)
			}
		})
	}
}

func TestNewCommandRESTConfig_ParsesOnceAndTunesSharedConfig(t *testing.T) {
	t.Helper()

	calls := 0
	source := &rest.Config{}
	load := transport.RESTConfigLoader(func(_ ...*pflag.FlagSet) (*rest.Config, error) {
		calls++

		return source, nil
	})

	got, err := newCommandRESTConfig(NewCommand(slog.Default()), load)
	if err != nil {
		t.Fatalf("newCommandRESTConfig: %v", err)
	}

	if calls != 1 {
		t.Fatalf("REST config parse calls = %d, want 1", calls)
	}

	if got != source {
		t.Fatal("newCommandRESTConfig replaced the parsed config")
	}

	if got.QPS != snapshotClientQPS || got.Burst != snapshotClientBurst ||
		got.Timeout != snapimport.DefaultControlRequestTimeout {
		t.Fatalf("shared tuning = QPS %v, Burst %d, Timeout %s; want QPS %v, Burst %d, Timeout %s",
			got.QPS, got.Burst, got.Timeout,
			snapshotClientQPS, snapshotClientBurst, snapimport.DefaultControlRequestTimeout)
	}
}

func TestNewCommand_NamespaceAndInputDefaults(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	ns, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagNamespace, err)
	}

	if ns != "" {
		t.Fatalf("default namespace: got %q, want empty string (namespace is required)", ns)
	}

	input, err := cmd.Flags().GetString(flagInput)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagInput, err)
	}

	if input != "" {
		t.Fatalf("default input: got %q, want empty string (input is required)", input)
	}
}

func TestRun_RequiresNamespace(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	if err := cmd.Flags().Set(flagInput, t.TempDir()); err != nil {
		t.Fatalf("setting %s flag: %v", flagInput, err)
	}

	err := Run(slog.Default(), cmd, nil)
	if err == nil {
		t.Fatal("expected error when namespace is empty, got nil")
	}

	if !strings.Contains(err.Error(), flagNamespace) {
		t.Fatalf("expected error to mention %q, got: %v", flagNamespace, err)
	}
}

func TestRun_RequiresInput(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	if err := cmd.Flags().Set(flagNamespace, "restored"); err != nil {
		t.Fatalf("setting %s flag: %v", flagNamespace, err)
	}

	err := Run(slog.Default(), cmd, nil)
	if err == nil {
		t.Fatal("expected error when input is empty, got nil")
	}

	if !strings.Contains(err.Error(), flagInput) {
		t.Fatalf("expected error to mention %q, got: %v", flagInput, err)
	}
}

func TestNewCommand_RejectsPositionalArgs(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	if err := cmd.Args(cmd, nil); err != nil {
		t.Fatalf("expected no error with zero positional args, got: %v", err)
	}

	if err := cmd.Args(cmd, []string{"unexpected"}); err == nil {
		t.Fatal("expected error with a positional arg, got nil")
	}
}

func TestNewCommand_NodeFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	node, err := cmd.Flags().GetString(flagNode)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagNode, err)
	}

	if node != "" {
		t.Fatalf("default --%s: got %q, want empty string", flagNode, node)
	}
}

func TestParseNodeFlag(t *testing.T) {
	t.Helper()

	cases := []struct {
		input     string
		wantKind  string
		wantName  string
		wantError bool
	}{
		{"", "", "", false},
		{"VolumeSnapshot/pvc-1", "VolumeSnapshot", "pvc-1", false},
		{"Snapshot/my-snap", "Snapshot", "my-snap", false},
		{"DemoVirtualMachineSnapshot/vm-abc", "DemoVirtualMachineSnapshot", "vm-abc", false},
		{"noslash", "", "", true},
		{"/noKind", "", "", true},
		{"noName/", "", "", true},
		{"Kind/extra/slash", "", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			kind, name, err := parseNodeFlag(tc.input)

			if tc.wantError {
				if err == nil {
					t.Fatalf("parseNodeFlag(%q): expected error, got nil", tc.input)
				}

				return
			}

			if err != nil {
				t.Fatalf("parseNodeFlag(%q): unexpected error: %v", tc.input, err)
			}

			if kind != tc.wantKind || name != tc.wantName {
				t.Errorf("parseNodeFlag(%q) = (%q, %q), want (%q, %q)", tc.input, kind, name, tc.wantKind, tc.wantName)
			}
		})
	}
}

func TestNewCommand_WorkersFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	workers, err := cmd.Flags().GetInt(flagWorkers)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagWorkers, err)
	}

	if workers != defaultImportWorkers {
		t.Fatalf("default --%s: got %d, want %d", flagWorkers, workers, defaultImportWorkers)
	}
}

func TestRun_NodeFlag_InvalidFormat(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	if err := cmd.Flags().Set(flagNamespace, "restored"); err != nil {
		t.Fatalf("setting %s flag: %v", flagNamespace, err)
	}

	if err := cmd.Flags().Set(flagInput, t.TempDir()); err != nil {
		t.Fatalf("setting %s flag: %v", flagInput, err)
	}

	if err := cmd.Flags().Set(flagNode, "badformat"); err != nil {
		t.Fatalf("setting %s flag: %v", flagNode, err)
	}

	err := Run(slog.Default(), cmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --node format, got nil")
	}

	if !strings.Contains(err.Error(), flagNode) {
		t.Errorf("expected error to mention %q, got: %v", flagNode, err)
	}
}

func TestNewCommand_AllowExistingFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	allowExisting, err := cmd.Flags().GetBool(flagAllowExisting)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagAllowExisting, err)
	}

	if allowExisting {
		t.Fatalf("default --%s: got true, want false (opt-in flag)", flagAllowExisting)
	}
}
