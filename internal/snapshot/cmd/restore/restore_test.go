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
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/restore"
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

func TestBoundedControlPlaneConfig(t *testing.T) {
	original := &rest.Config{
		Host:    "https://cluster.example.test",
		Timeout: 0,
	}

	bounded := boundedControlPlaneConfig(original)

	if bounded == original {
		t.Fatal("boundedControlPlaneConfig returned the input pointer")
	}

	if bounded.Host != original.Host {
		t.Fatalf("bounded host = %q, want %q", bounded.Host, original.Host)
	}

	if bounded.Timeout != restore.DefaultControlPlaneTimeout {
		t.Fatalf(
			"bounded timeout = %s, want %s",
			bounded.Timeout,
			restore.DefaultControlPlaneTimeout,
		)
	}

	if original.Timeout != 0 {
		t.Fatalf("original timeout = %s, want zero", original.Timeout)
	}

	transport, ok := bounded.WrapTransport(&http.Transport{}).(*http.Transport)
	if !ok {
		t.Fatalf("bounded transport has type %T, want *http.Transport", transport)
	}

	if transport.DialContext == nil {
		t.Fatal("bounded transport has no DialContext")
	}

	if transport.TLSHandshakeTimeout != restore.DefaultControlPlaneTimeout {
		t.Errorf(
			"TLS handshake timeout = %s, want %s",
			transport.TLSHandshakeTimeout,
			restore.DefaultControlPlaneTimeout,
		)
	}

	if transport.ResponseHeaderTimeout != restore.DefaultControlPlaneTimeout {
		t.Errorf(
			"response header timeout = %s, want %s",
			transport.ResponseHeaderTimeout,
			restore.DefaultControlPlaneTimeout,
		)
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

func TestNewCommand_DryRunFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	dryRun, err := cmd.Flags().GetBool(flagDryRun)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagDryRun, err)
	}

	if dryRun {
		t.Fatalf("default --%s: got true, want false", flagDryRun)
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

func TestNewCommand_NodeFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	node, err := cmd.Flags().GetString(flagNode)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagNode, err)
	}

	if node != "" {
		t.Fatalf("default --node: got %q, want empty string (full-tree restore by default)", node)
	}

	apiVersion, err := cmd.Flags().GetString(flagNodeAPIVersion)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagNodeAPIVersion, err)
	}

	if apiVersion != "" {
		t.Fatalf("default --%s: got %q, want empty string", flagNodeAPIVersion, apiVersion)
	}
}

func TestNewCommand_NodeHelpExplainsMissingChildProof(t *testing.T) {
	t.Parallel()

	longHelp := NewCommand(slog.Default()).Long
	for _, text := range []string{
		"identity remains restorable",
		"belongs to the tree but is",
		"Original-source selection fails closed",
		"--node-api-version",
	} {
		if !strings.Contains(longHelp, text) {
			t.Errorf("long help does not contain %q", text)
		}
	}
}

func TestParseNodeFlag(t *testing.T) {
	t.Helper()

	cases := []struct {
		input    string
		wantKind string
		wantName string
		wantErr  bool
	}{
		// empty → full tree
		{"", "", "", false},
		// valid simple kind/name
		{"Snapshot/my-snap", "Snapshot", "my-snap", false},
		// domain kind with UUID-style name
		{"DemoVirtualDiskSnapshot/nss-child-abc123", "DemoVirtualDiskSnapshot", "nss-child-abc123", false},
		// VolumeSnapshot leaf
		{"VolumeSnapshot/demo-pvc", "VolumeSnapshot", "demo-pvc", false},
		// no slash → error
		{"NoSlashHere", "", "", true},
		// leading slash (empty kind) → error
		{"/name", "", "", true},
		// trailing slash (empty name) → error
		{"Kind/", "", "", true},
		// two slashes → error
		{"Kind/a/b", "", "", true},
		// just a slash → error (empty kind)
		{"/", "", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			gotKind, gotName, err := parseNodeFlag(tc.input)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("input %q: expected error, got nil (kind=%q name=%q)", tc.input, gotKind, gotName)
				}

				return
			}

			if err != nil {
				t.Fatalf("input %q: unexpected error: %v", tc.input, err)
			}

			if gotKind != tc.wantKind {
				t.Fatalf("input %q: got kind=%q, want %q", tc.input, gotKind, tc.wantKind)
			}

			if gotName != tc.wantName {
				t.Fatalf("input %q: got name=%q, want %q", tc.input, gotName, tc.wantName)
			}
		})
	}
}

func TestRun_NodeFlag_InvalidFormat(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	if err := cmd.Flags().Set(flagNamespace, "test-ns"); err != nil {
		t.Fatalf("setting namespace flag: %v", err)
	}

	if err := cmd.Flags().Set(flagNode, "NoSlashHere"); err != nil {
		t.Fatalf("setting node flag: %v", err)
	}

	err := Run(slog.Default(), cmd, []string{"my-snap"})
	if err == nil {
		t.Fatal("expected error for invalid --node format, got nil")
	}

	if !strings.Contains(err.Error(), flagNode) {
		t.Fatalf("error should mention %q, got: %v", flagNode, err)
	}
}

func TestRun_NodeAPIVersionFlagValidation(t *testing.T) {
	tests := []struct {
		name              string
		node              string
		apiVersion        string
		wantValidationErr string
	}{
		{
			name:              "api version without node",
			apiVersion:        "apps/v1",
			wantValidationErr: "--node-api-version requires --node",
		},
		{
			name:              "empty named group",
			node:              "Deployment/demo",
			apiVersion:        "/v1",
			wantValidationErr: "invalid --node-api-version",
		},
		{
			name:              "missing named version",
			node:              "Deployment/demo",
			apiVersion:        "apps/",
			wantValidationErr: "invalid --node-api-version",
		},
		{
			name:              "too many separators",
			node:              "Deployment/demo",
			apiVersion:        "apps/example/v1",
			wantValidationErr: "invalid --node-api-version",
		},
		{
			name:              "invalid group",
			node:              "Deployment/demo",
			apiVersion:        "Apps/v1",
			wantValidationErr: "invalid --node-api-version",
		},
		{
			name:       "core version",
			node:       "PersistentVolumeClaim/demo",
			apiVersion: "v1",
		},
		{
			name:       "named group version",
			node:       "Deployment/demo",
			apiVersion: "apps/v1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewCommand(slog.Default())

			if err := cmd.Flags().Set(flagNamespace, "test-ns"); err != nil {
				t.Fatalf("setting namespace flag: %v", err)
			}

			if tc.node != "" {
				if err := cmd.Flags().Set(flagNode, tc.node); err != nil {
					t.Fatalf("setting node flag: %v", err)
				}
			}

			if err := cmd.Flags().Set(flagNodeAPIVersion, tc.apiVersion); err != nil {
				t.Fatalf("setting node API version flag: %v", err)
			}

			err := Run(slog.Default(), cmd, []string{"my-snap"})
			if err == nil {
				t.Fatal("Run unexpectedly succeeded without a configured cluster")
			}

			if tc.wantValidationErr != "" {
				if !strings.Contains(err.Error(), tc.wantValidationErr) {
					t.Fatalf("Run error = %q, want validation text %q", err, tc.wantValidationErr)
				}

				return
			}

			if strings.Contains(err.Error(), "--"+flagNodeAPIVersion) {
				t.Fatalf("valid API version unexpectedly rejected: %v", err)
			}
		})
	}
}

func TestNewCommand_ScopeFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	scope, err := cmd.Flags().GetString(flagScope)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagScope, err)
	}

	if scope != "subtree" {
		t.Fatalf("default --%s: got %q, want %q", flagScope, scope, "subtree")
	}
}

func TestNewCommand_ObjectFlagDefault(t *testing.T) {
	t.Helper()

	cmd := NewCommand(slog.Default())

	object, err := cmd.Flags().GetString(flagObject)
	if err != nil {
		t.Fatalf("getting %s flag: %v", flagObject, err)
	}

	if object != "" {
		t.Fatalf("default --%s: got %q, want empty string (no object filter)", flagObject, object)
	}
}

// TestRun_ScopeObjectFlags_ValidAndInvalidCombinations exercises the client-side validation
// of --scope/--object before Run reaches the network: an unknown --scope value, --object
// used without --scope node, and every valid combination (default, --scope node alone,
// --scope node with --object).
func TestRun_ScopeObjectFlags_ValidAndInvalidCombinations(t *testing.T) {
	t.Helper()

	tests := []struct {
		name        string
		scope       string
		object      string
		wantErr     bool
		wantErrText string
	}{
		{
			name:  "neither flag set: default scope=subtree, no object filter",
			scope: "", object: "",
		},
		{
			name:  "scope=node, no object filter (whole node, no children)",
			scope: "node", object: "",
		},
		{
			name:  "scope=node with object filter",
			scope: "node", object: "PersistentVolumeClaim/bk-disk-a",
		},
		{
			name:        "unknown scope value rejected client-side",
			scope:       "bogus",
			wantErr:     true,
			wantErrText: flagScope,
		},
		{
			name:        "object without scope=node rejected client-side",
			scope:       "subtree",
			object:      "PersistentVolumeClaim/bk-disk-a",
			wantErr:     true,
			wantErrText: flagObject,
		},
		{
			name:        "object with default (unset) scope rejected client-side",
			object:      "PersistentVolumeClaim/bk-disk-a",
			wantErr:     true,
			wantErrText: flagObject,
		},
		{
			name:        "malformed object flag rejected client-side",
			scope:       "node",
			object:      "NoSlashHere",
			wantErr:     true,
			wantErrText: flagObject,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()

			cmd := NewCommand(slog.Default())

			if err := cmd.Flags().Set(flagNamespace, "test-ns"); err != nil {
				t.Fatalf("setting namespace flag: %v", err)
			}

			if tc.scope != "" {
				if err := cmd.Flags().Set(flagScope, tc.scope); err != nil {
					t.Fatalf("setting scope flag: %v", err)
				}
			}

			if tc.object != "" {
				if err := cmd.Flags().Set(flagObject, tc.object); err != nil {
					t.Fatalf("setting object flag: %v", err)
				}
			}

			// Run always fails past validation in this unit test (no live cluster to build
			// clients against); assert on the validation-stage error only by requiring the
			// invalid cases to fail with a specific message and the valid cases to get past
			// flag validation (i.e. not fail with a --scope/--object-shaped message).
			err := Run(slog.Default(), cmd, []string{"my-snap"})

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected a validation error, got nil")
				}

				if !strings.Contains(err.Error(), tc.wantErrText) {
					t.Fatalf("error should mention %q, got: %v", tc.wantErrText, err)
				}

				return
			}

			if err != nil && (strings.Contains(err.Error(), "invalid --"+flagScope) || strings.Contains(err.Error(), "--"+flagObject+" requires")) {
				t.Fatalf("valid combination unexpectedly rejected by flag validation: %v", err)
			}
		})
	}
}
