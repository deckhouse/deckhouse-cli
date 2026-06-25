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
	"log/slog"
	"strings"
	"testing"
	"time"
)

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
