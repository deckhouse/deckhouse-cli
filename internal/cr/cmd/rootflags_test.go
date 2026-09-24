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

package cr

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

// runPreRun builds a fresh root with setupRootFlags, parses the given args,
// and invokes PersistentPreRunE. Returns the resulting *Options so each test
// can inspect the side-effects of the flag.
func runPreRun(t *testing.T, args []string) *registry.Options {
	t.Helper()

	opts := registry.New()
	cmd := &cobra.Command{Use: "cr"}
	setupRootFlags(cmd, opts)

	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}

	if err := cmd.PersistentPreRunE(cmd, nil); err != nil {
		t.Fatalf("PersistentPreRunE: %v", err)
	}

	return opts
}

func TestInsecureFlag_Off(t *testing.T) {
	opts := runPreRun(t, nil)
	if opts.PlainHTTP || opts.TLSSkipVerify {
		t.Fatalf("expected TLS to be enforced without --insecure; got %+v", opts)
	}
}

// --insecure has always meant both "talk HTTP" and "accept any certificate".
// The client separates the two, so this guards the flag against quietly
// covering only one of them if it is ever split.
func TestInsecureFlag_On(t *testing.T) {
	opts := runPreRun(t, []string{"--insecure"})
	if !opts.PlainHTTP {
		t.Errorf("--insecure should permit plain HTTP")
	}

	if !opts.TLSSkipVerify {
		t.Errorf("--insecure should skip TLS verification")
	}
}

func TestPlatformFlag(t *testing.T) {
	opts := runPreRun(t, []string{"--platform", "linux/arm64/v8"})
	if opts.Platform == nil {
		t.Fatalf("--platform was not applied")
	}

	if got := opts.Platform.String(); got != "linux/arm64/v8" {
		t.Errorf("platform = %q, want linux/arm64/v8", got)
	}
}

func TestPlatformFlag_RejectsGarbage(t *testing.T) {
	opts := registry.New()
	cmd := &cobra.Command{Use: "cr"}
	setupRootFlags(cmd, opts)

	if err := cmd.ParseFlags([]string{"--platform", "not//a//platform"}); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}

	if err := cmd.PersistentPreRunE(cmd, nil); err == nil {
		t.Fatalf("expected an error for an unparseable --platform")
	}
}

// Credentials become an authenticator rather than a keychain, so they reach
// only the registry the user named: the client is built per reference, and the
// old static keychain answered for any host it was asked about.
func TestCredentialFlags_BecomeAuth(t *testing.T) {
	opts := runPreRun(t, []string{"--username", "robot", "--password", "s3cret"})
	if opts.Auth == nil {
		t.Fatalf("--username/--password should install an authenticator")
	}

	cfg, err := opts.Auth.Authorization()
	if err != nil {
		t.Fatalf("Authorization: %v", err)
	}

	if cfg.Username != "robot" || cfg.Password != "s3cret" {
		t.Errorf("authenticator carries %+v, want robot/s3cret", cfg)
	}
}

// Half a credential pair would otherwise be dropped silently and the command
// would fall back to the Docker config, masking the mistake with a 401.
func TestCredentialFlags_MustBePaired(t *testing.T) {
	for _, args := range [][]string{{"--username", "robot"}, {"--password", "s3cret"}} {
		opts := registry.New()
		cmd := &cobra.Command{Use: "cr"}
		setupRootFlags(cmd, opts)

		if err := cmd.ParseFlags(args); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}

		err := cmd.PersistentPreRunE(cmd, nil)
		if err == nil {
			t.Fatalf("%v: expected an error for a lone credential flag", args)
		}

		if !strings.Contains(err.Error(), "must be used together") {
			t.Errorf("%v: unexpected error: %v", args, err)
		}
	}
}

// login reads the credential flags itself and prompts for whichever half is
// missing, so the pairing rule must not apply to it.
func TestCredentialFlags_LoginIsExempt(t *testing.T) {
	opts := registry.New()
	root := &cobra.Command{Use: "cr"}
	setupRootFlags(root, opts)

	login := &cobra.Command{Use: "login"}
	root.AddCommand(login)

	if err := root.ParseFlags([]string{"--username", "robot"}); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}

	if err := root.PersistentPreRunE(login, nil); err != nil {
		t.Errorf("login must tolerate a lone --username: %v", err)
	}
}

// PersistentPreRunE resets opts before applying the flags, so re-entry (a test
// harness, an embedder, a retry) starts from a clean state instead of merging
// into whatever a previous run left behind.
func TestPersistentPreRunE_IsIdempotent(t *testing.T) {
	opts := registry.New()
	cmd := &cobra.Command{Use: "cr"}
	setupRootFlags(cmd, opts)

	if err := cmd.ParseFlags([]string{"--insecure", "--platform", "linux/arm64"}); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}

	if err := cmd.PersistentPreRunE(cmd, nil); err != nil {
		t.Fatalf("first PersistentPreRunE: %v", err)
	}

	first := *opts

	if err := cmd.PersistentPreRunE(cmd, nil); err != nil {
		t.Fatalf("second PersistentPreRunE: %v", err)
	}

	if opts.PlainHTTP != first.PlainHTTP || opts.TLSSkipVerify != first.TLSSkipVerify {
		t.Errorf("insecure flags drifted across re-entry: %+v then %+v", first, *opts)
	}

	if opts.Platform == nil || opts.Platform.String() != first.Platform.String() {
		t.Errorf("platform drifted across re-entry: %+v then %+v", first.Platform, opts.Platform)
	}
}
