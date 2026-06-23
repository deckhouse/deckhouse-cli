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

package restorecmd

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestNewCommand_Defaults(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	wantUse := cmdUse + " [flags] <snapshot>"
	if cmd.Use != wantUse {
		t.Fatalf("unexpected Use: got %q, want %q", cmd.Use, wantUse)
	}

	if !cmd.SilenceUsage {
		t.Fatal("SilenceUsage should be true")
	}

	if !cmd.SilenceErrors {
		t.Fatal("SilenceErrors should be true")
	}

	wait, err := cmd.Flags().GetBool(flagWait)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagWait, err)
	}

	if wait {
		t.Fatalf("default --%s: got true, want false", flagWait)
	}

	timeout, err := cmd.Flags().GetDuration(flagTimeout)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagTimeout, err)
	}

	if timeout != 10*time.Minute {
		t.Fatalf("default --%s: got %s, want 10m", flagTimeout, timeout)
	}
}

func TestNewCommand_NamespaceFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	ns, err := cmd.Flags().GetString(flagNamespace)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagNamespace, err)
	}

	if ns != "" {
		t.Fatalf("default namespace: got %q, want empty string (namespace is required)", ns)
	}
}

func TestRun_RequiresNamespace(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	err := Run(slog.Default(), cmd, []string{"my-snap"})
	if err == nil {
		t.Fatal("expected error when namespace is empty, got nil")
	}

	if !strings.Contains(err.Error(), flagNamespace) {
		t.Fatalf("expected error to mention %q, got: %v", flagNamespace, err)
	}
}

func TestNewCommand_RequiresExactlyOneArg(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	if err := cmd.Args(cmd, []string{}); err == nil {
		t.Fatal("expected error with zero positional args, got nil")
	}

	if err := cmd.Args(cmd, []string{"my-snap"}); err != nil {
		t.Fatalf("expected no error with one positional arg, got: %v", err)
	}

	if err := cmd.Args(cmd, []string{"a", "b"}); err == nil {
		t.Fatal("expected error with two positional args, got nil")
	}
}
