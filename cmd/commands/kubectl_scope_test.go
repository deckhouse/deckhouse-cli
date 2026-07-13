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
	"net/http"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"
)

func TestParseScopeFlag(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		wantKind    string
		wantProject string
		wantErr     bool
	}{
		{name: "not passed at all", raw: "", wantKind: "", wantProject: ""},
		{name: "accessible", raw: "accessible", wantKind: "accessible"},
		{name: "projects", raw: "projects", wantKind: "projects"},
		{name: "system", raw: "system", wantKind: "system"},
		{name: "project with name", raw: "project:myproject", wantKind: "project", wantProject: "myproject"},
		{name: "project without name is an error", raw: "project:", wantErr: true},
		{name: "unknown value is an error", raw: "bogus", wantErr: true},
		// "user" was the pre-review name of the "projects" scope; it must be a
		// clean error now, not a silently-working alias.
		{name: "renamed-away user value is an error", raw: "user", wantErr: true},
		// The following two are valid values for the OTHER, unrelated --scope
		// flag on `d8 iam access grant/revoke` (cluster|all-namespaces|labels=K=V).
		// Pasting one here must be a clean "invalid value" error, never a silent
		// (mis)parse -- see scopeFlagName's doc.
		{name: "iam's cluster value must be rejected here", raw: "cluster", wantErr: true},
		{name: "iam's all-namespaces value must be rejected here", raw: "all-namespaces", wantErr: true},
		{name: "iam's labels=K=V value must be rejected here", raw: "labels=env=prod", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, project, err := parseScopeFlag(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseScopeFlag(%q): expected error, got nil (kind=%q, project=%q)", tt.raw, kind, project)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseScopeFlag(%q): unexpected error: %v", tt.raw, err)
			}
			if kind != tt.wantKind || project != tt.wantProject {
				t.Fatalf("parseScopeFlag(%q) = (%q, %q), want (%q, %q)", tt.raw, kind, project, tt.wantKind, tt.wantProject)
			}
		})
	}
}

// TestScopeUsageError covers the guard that keeps --scope from being used in a
// context where it cannot work: it needs the cluster-scoped request URL that
// -A/--all-namespaces produces, and an explicit -n/--namespace defeats it. The
// -n conflict is reported in preference to the missing -A, since that is the
// more specific mistake.
func TestScopeUsageError(t *testing.T) {
	// mkCmd builds a command carrying the same two flags scopeUsageError reads.
	// allNsVal is the explicit command-line value of --all-namespaces: "" means
	// the flag was not passed at all, "true"/"false" mean it was passed with
	// that value (both mark the flag as "changed" -- which is exactly the trap
	// the guard must not fall into).
	mkCmd := func(hasAllNs bool, allNsVal string, nsSet bool) *cobra.Command {
		cmd := &cobra.Command{Use: "get"}
		if hasAllNs {
			cmd.Flags().BoolP("all-namespaces", "A", false, "")
		}
		cmd.Flags().StringP("namespace", "n", "", "")
		if allNsVal != "" {
			_ = cmd.Flags().Set("all-namespaces", allNsVal)
		}
		if nsSet {
			_ = cmd.Flags().Set("namespace", "demo")
		}
		return cmd
	}

	tests := []struct {
		name     string
		scope    string
		hasAllNs bool
		allNsVal string
		nsSet    bool
		wantErr  string // substring; "" means expect no error
	}{
		{name: "empty scope is always fine", scope: "", hasAllNs: true},
		{name: "empty scope fine even with -n", scope: "", hasAllNs: true, nsSet: true},
		{name: "scope with -A and no -n is valid", scope: "system", hasAllNs: true, allNsVal: "true"},
		{name: "scope without -A errors", scope: "system", hasAllNs: true, wantErr: "requires -A"},
		{name: "scope with explicit --all-namespaces=false is treated as missing -A", scope: "system", hasAllNs: true, allNsVal: "false", wantErr: "requires -A"},
		{name: "scope with -n errors even without -A", scope: "system", hasAllNs: true, nsSet: true, wantErr: "cannot be combined with -n"},
		{name: "scope with both -A and -n errors on -n (more specific)", scope: "system", hasAllNs: true, allNsVal: "true", nsSet: true, wantErr: "cannot be combined with -n"},
		{name: "command lacking an -A flag errors as requires -A", scope: "system", hasAllNs: false, wantErr: "requires -A"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := scopeUsageError(mkCmd(tt.hasAllNs, tt.allNsVal, tt.nsSet), tt.scope)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("scopeUsageError: unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("scopeUsageError: expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("scopeUsageError error = %q, want it to contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestScopeForbiddenHint covers the hint appended to kubectl's cluster-scope
// Forbidden error when the invocation was a `get -A` without --scope.
func TestScopeForbiddenHint(t *testing.T) {
	const forbiddenMsg = `Error from server (Forbidden): deployments.apps is forbidden: User "u" cannot list resource "deployments" in API group "apps" at the cluster scope`

	t.Run("not eligible yields no hint", func(t *testing.T) {
		if got := scopeForbiddenHint(forbiddenMsg, false, "deployments"); got != "" {
			t.Fatalf("expected no hint when not eligible, got %q", got)
		}
	})

	t.Run("eligible but unrelated error yields no hint", func(t *testing.T) {
		for _, msg := range []string{
			"Error from server (NotFound): the server could not find the requested resource",
			`Error from server (Forbidden): pods is forbidden: User "u" cannot list resource "pods" in the namespace "default"`, // namespaced 403, not cluster scope
			"unable to connect to the server: dial tcp: lookup nope: no such host",
		} {
			if got := scopeForbiddenHint(msg, true, "deployments"); got != "" {
				t.Fatalf("expected no hint for %q, got %q", msg, got)
			}
		}
	})

	t.Run("eligible cluster-scope forbidden yields hint with the actual resource", func(t *testing.T) {
		got := scopeForbiddenHint(forbiddenMsg, true, "deployments")
		if got == "" {
			t.Fatal("expected a hint, got none")
		}
		for _, want := range []string{
			"--scope=accessible",
			"--scope=projects",
			"--scope=project:<name>",
			"--scope=system",
			"d8 k get deployments -A",
			// A cluster-scoped resource (e.g. nodes) produces the same 403
			// shape, and --scope cannot help it; the wording must stay
			// conditional so the hint is not an outright lie there.
			"If\nthis is a namespaced resource",
		} {
			if !strings.Contains(got, want) {
				t.Fatalf("hint missing %q:\n%s", want, got)
			}
		}
	})

	t.Run("unknown resource falls back to a placeholder, never a guess", func(t *testing.T) {
		got := scopeForbiddenHint(forbiddenMsg, true, "")
		if !strings.Contains(got, "d8 k get <resource> -A") {
			t.Fatalf("expected <resource> placeholder, got:\n%s", got)
		}
		if strings.Contains(got, "get pods") {
			t.Fatalf("hint must not guess a resource name:\n%s", got)
		}
	})
}

// TestRewriteD8Commands pins the two message shapes the rewrite must
// distinguish: kubectl builds suggestions either from os.Args[0] ("d8 get"),
// which needs `k` inserted, or from cmd.CommandPath() ("d8 k get"), which is
// already correct -- a naive regex replace turned the latter into
// "d8 k k get" (caught in review).
func TestRewriteD8Commands(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "os.Args[0]-built suggestion gets k inserted",
			in:   "You can run `d8 replace -f ...` to try this update again.",
			want: "You can run `d8 k replace -f ...` to try this update again.",
		},
		{
			name: "CommandPath-built suggestion stays untouched",
			in:   "See 'd8 k get -h' for help and examples",
			want: "See 'd8 k get -h' for help and examples",
		},
		{
			name: "double-quoted CommandPath form stays untouched",
			in:   `Use "d8 k explain <resource>" for a detailed description`,
			want: `Use "d8 k explain <resource>" for a detailed description`,
		},
		{
			name: "mixed message rewrites only the bare form",
			in:   "See 'd8 get -h'. Also see 'd8 k get -h'.",
			want: "See 'd8 k get -h'. Also see 'd8 k get -h'.",
		},
		{
			name: "unquoted d8 references are not touched at all",
			in:   "d8 get pods is not quoted here",
			want: "d8 get pods is not quoted here",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rewriteD8Commands(tt.in); got != tt.want {
				t.Fatalf("rewriteD8Commands(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestScopeCompletionOfflinePaths pins the no-cluster contract of --scope
// completion: anything short of a full "project:" prefix must be served
// without any network round-trip -- a tab press on a slow or unreachable
// cluster must never freeze the shell just to show static enum values.
// Passing nil configFlags proves it: any code path that reached for the
// cluster would panic on the nil receiver.
func TestScopeCompletionOfflinePaths(t *testing.T) {
	tests := []struct {
		name       string
		toComplete string
		want       []string
	}{
		{name: "bare tab lists statics plus the project: literal", toComplete: "", want: []string{"accessible", "projects", "system", "project:"}},
		{name: "prefix of a static value", toComplete: "sys", want: []string{"system"}},
		{name: "prefix on the project: path offers the literal, no fetch", toComplete: "pro", want: []string{"projects", "project:"}},
		{name: "full static value", toComplete: "accessible", want: []string{"accessible"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, directive := scopeCompletion(nil, tt.toComplete)
			if directive != cobra.ShellCompDirectiveNoFileComp {
				t.Fatalf("directive = %v, want ShellCompDirectiveNoFileComp", directive)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("candidates = %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("candidates = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// recordingRoundTripper captures the last request's headers without making
// any real network call.
type recordingRoundTripper struct {
	lastHeaders http.Header
	calls       int
}

func (r *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	r.lastHeaders = req.Header.Clone()
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestWrapConfigWithScope_InjectsBothHeadersForProject(t *testing.T) {
	rec := &recordingRoundTripper{}
	wrapped := wrapConfigWithScope(&rest.Config{}, "project", "myproject")

	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/api/v1/pods", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if _, err := wrapped.WrapTransport(rec).RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}

	if got := rec.lastHeaders.Get(scopeHeaderName); got != "project" {
		t.Fatalf("%s header = %q, want %q", scopeHeaderName, got, "project")
	}
	if got := rec.lastHeaders.Get(scopeProjectHeaderName); got != "myproject" {
		t.Fatalf("%s header = %q, want %q", scopeProjectHeaderName, got, "myproject")
	}
	// The original *http.Request must never be mutated -- RoundTrippers that
	// share a request across retries/redirects would otherwise leak our
	// headers into unrelated calls.
	if got := req.Header.Get(scopeHeaderName); got != "" {
		t.Fatalf("original request was mutated in place; got %s=%q, want no header set", scopeHeaderName, got)
	}
}

func TestWrapConfigWithScope_NoProjectHeaderForNonProjectKind(t *testing.T) {
	rec := &recordingRoundTripper{}
	wrapped := wrapConfigWithScope(&rest.Config{}, "accessible", "")

	req, _ := http.NewRequest(http.MethodGet, "http://example.invalid/api/v1/pods", nil)
	if _, err := wrapped.WrapTransport(rec).RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}

	if got := rec.lastHeaders.Get(scopeHeaderName); got != "accessible" {
		t.Fatalf("%s header = %q, want %q", scopeHeaderName, got, "accessible")
	}
	if got := rec.lastHeaders.Get(scopeProjectHeaderName); got != "" {
		t.Fatalf("%s header should be absent, got %q", scopeProjectHeaderName, got)
	}
}

// TestWrapConfigWithScope_ComposesWithExistingWrapTransport is the direct
// regression test for the plan's "verify interaction with an exec-credential
// auth plugin" requirement: exec-credential auth is implemented via the same
// rest.Config.WrapTransport mechanism we use, so a naive implementation could
// silently clobber it instead of composing.
func TestWrapConfigWithScope_ComposesWithExistingWrapTransport(t *testing.T) {
	rec := &recordingRoundTripper{}
	authInjected := false

	cfg := &rest.Config{
		WrapTransport: func(rt http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				authInjected = true
				req = req.Clone(req.Context())
				req.Header.Set("Authorization", "Bearer exec-credential-token")
				return rt.RoundTrip(req)
			})
		},
	}

	wrapped := wrapConfigWithScope(cfg, "system", "")
	req, _ := http.NewRequest(http.MethodGet, "http://example.invalid/api/v1/pods", nil)
	if _, err := wrapped.WrapTransport(rec).RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}

	if !authInjected {
		t.Fatal("pre-existing WrapTransport (exec-credential simulation) was never invoked -- our wrapping clobbered it instead of composing")
	}
	if got := rec.lastHeaders.Get(scopeHeaderName); got != "system" {
		t.Fatalf("%s header = %q, want %q", scopeHeaderName, got, "system")
	}
	if got := rec.lastHeaders.Get("Authorization"); got != "Bearer exec-credential-token" {
		t.Fatalf("Authorization header = %q, want the exec-credential one to survive composition", got)
	}
}

// TestWrapConfigWithScope_OriginalConfigUntouched guards against a
// copy-paste bug where the wrapper mutates the caller's *rest.Config in
// place instead of operating on rest.CopyConfig's result -- ConfigFlags'
// WithWrapConfigFn is called once per client built from the same base
// config, so mutating the original would leak scope headers into unrelated
// clients built later without --scope.
func TestWrapConfigWithScope_OriginalConfigUntouched(t *testing.T) {
	cfg := &rest.Config{}
	_ = wrapConfigWithScope(cfg, "accessible", "")
	if cfg.WrapTransport != nil {
		t.Fatal("wrapConfigWithScope mutated the caller's *rest.Config in place; it must operate on a copy")
	}
}
