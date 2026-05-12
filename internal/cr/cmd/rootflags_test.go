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
	"reflect"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
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

// hasInsecureName looks for the exact name.Insecure marker in opts.Name.
// name.Option is a func type, so values are not directly comparable - we
// compare function pointers via reflect. This is more precise than counting
// slice length, which would silently break if a future default option were
// added to New().
func hasInsecureName(opts *registry.Options) bool {
	want := reflect.ValueOf(name.Insecure).Pointer()
	for _, opt := range opts.Name {
		if reflect.ValueOf(opt).Pointer() == want {
			return true
		}
	}
	return false
}

func TestInsecureFlag_Off(t *testing.T) {
	opts := runPreRun(t, nil)
	if hasInsecureName(opts) {
		t.Fatalf("expected insecure off when --insecure is not passed")
	}
}

func TestInsecureFlag_On(t *testing.T) {
	opts := runPreRun(t, []string{"--insecure"})
	if !hasInsecureName(opts) {
		t.Fatalf("expected insecure on when --insecure is passed")
	}
}

// PersistentPreRunE must reset opts before applying flag-driven mutators,
// so re-entry (test harness, embedder, retry) cannot double-append to
// opts.Remote / opts.Name and end up with a malformed merge of options.
func TestPersistentPreRunE_IsIdempotent(t *testing.T) {
	opts := registry.New()
	cmd := &cobra.Command{Use: "cr"}
	setupRootFlags(cmd, opts)
	if err := cmd.ParseFlags([]string{"--insecure"}); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}

	if err := cmd.PersistentPreRunE(cmd, nil); err != nil {
		t.Fatalf("first PersistentPreRunE: %v", err)
	}
	firstName := len(opts.Name)
	firstRemote := len(opts.Remote)

	if err := cmd.PersistentPreRunE(cmd, nil); err != nil {
		t.Fatalf("second PersistentPreRunE: %v", err)
	}
	if got := len(opts.Name); got != firstName {
		t.Errorf("opts.Name grew on re-entry: was %d, now %d", firstName, got)
	}
	if got := len(opts.Remote); got != firstRemote {
		t.Errorf("opts.Remote grew on re-entry: was %d, now %d", firstRemote, got)
	}
	if !hasInsecureName(opts) {
		t.Errorf("insecure flag was lost across re-entry")
	}
}
