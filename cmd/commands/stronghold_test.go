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

package commands

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestNewStrongholdCommand_DisableFlagParsing(t *testing.T) {
	cmd := NewStrongholdCommand()
	if !cmd.DisableFlagParsing {
		t.Error("DisableFlagParsing must be true so that Stronghold flags (-method, -path, etc.) are not intercepted by cobra")
	}
}

func TestNewStrongholdCommand_Metadata(t *testing.T) {
	cmd := NewStrongholdCommand()

	if cmd.Use != "stronghold [command] [args...]" {
		t.Errorf("Use: expected %q, got %q", "stronghold [command] [args...]", cmd.Use)
	}
	if cmd.Short == "" {
		t.Error("Short must not be empty")
	}
	if !cmd.SilenceErrors {
		t.Error("SilenceErrors must be true")
	}
	if !cmd.SilenceUsage {
		t.Error("SilenceUsage must be true")
	}
}

// Verifies that Stronghold-style flags (single dash) are correctly
// passed through as args and not parsed by cobra as its own flags.
func TestStrongholdCommand_FlagsPassedAsArgs(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		expectedArgs []string
	}{
		{
			name:         "login with -method and -path",
			args:         []string{"login", "-method=oidc", "-path=oidc_deckhouse", "-token-only"},
			expectedArgs: []string{"login", "-method=oidc", "-path=oidc_deckhouse", "-token-only"},
		},
		{
			name:         "login with space-separated flags",
			args:         []string{"login", "-method", "oidc", "-path", "oidc_deckhouse"},
			expectedArgs: []string{"login", "-method", "oidc", "-path", "oidc_deckhouse"},
		},
		{
			name:         "kv put with path and data",
			args:         []string{"kv", "put", "secret/my-secret", "key=value"},
			expectedArgs: []string{"kv", "put", "secret/my-secret", "key=value"},
		},
		{
			name:         "status without arguments",
			args:         []string{"status"},
			expectedArgs: []string{"status"},
		},
		{
			name:         "-format flag",
			args:         []string{"status", "-format=json"},
			expectedArgs: []string{"status", "-format=json"},
		},
		{
			name:         "-address flag",
			args:         []string{"login", "-method=oidc", "-address=https://stronghold.example.com"},
			expectedArgs: []string{"login", "-method=oidc", "-address=https://stronghold.example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedArgs []string

			cmd := &cobra.Command{
				Use:                "stronghold [command] [args...]",
				DisableFlagParsing: true,
				SilenceErrors:      true,
				SilenceUsage:       true,
				Run: func(_ *cobra.Command, args []string) {
					capturedArgs = args
				},
			}

			root := &cobra.Command{Use: "d8"}
			root.AddCommand(cmd)
			root.SetArgs(append([]string{"stronghold"}, tt.args...))

			if err := root.Execute(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(capturedArgs) != len(tt.expectedArgs) {
				t.Fatalf("args count: expected %d, got %d\nexpected: %v\ngot:      %v",
					len(tt.expectedArgs), len(capturedArgs), tt.expectedArgs, capturedArgs)
			}

			for i, expected := range tt.expectedArgs {
				if capturedArgs[i] != expected {
					t.Errorf("arg [%d]: expected %q, got %q", i, expected, capturedArgs[i])
				}
			}
		})
	}
}

func TestStrongholdCommand_NoShorthandFlagError(t *testing.T) {
	cmd := &cobra.Command{
		Use:                "stronghold [command] [args...]",
		DisableFlagParsing: true,
		SilenceErrors:      true,
		Run: func(_ *cobra.Command, _ []string) {},
	}

	root := &cobra.Command{Use: "d8"}
	root.AddCommand(cmd)
	root.SetArgs([]string{"stronghold", "login", "-method=oidc", "-path=oidc_deckhouse", "-token-only"})

	err := root.Execute()
	if err != nil {
		t.Errorf("command must not return error with Stronghold-style flags, but got: %v", err)
	}
}

// Proves the bug: without DisableFlagParsing, cobra interprets
// single-dash flags as shorthand and fails.
func TestStrongholdCommand_FailsWithoutDisableFlagParsing(t *testing.T) {
	cmd := &cobra.Command{
		Use:                "stronghold [command] [args...]",
		DisableFlagParsing: false,
		SilenceErrors:      true,
		SilenceUsage:       true,
		Run:                func(_ *cobra.Command, _ []string) {},
	}

	root := &cobra.Command{Use: "d8"}
	root.AddCommand(cmd)
	root.SetArgs([]string{"stronghold", "login", "-method=oidc", "-path=oidc_deckhouse", "-token-only"})

	err := root.Execute()
	if err == nil {
		t.Error("expected error when DisableFlagParsing is false, but got nil")
	}
}
