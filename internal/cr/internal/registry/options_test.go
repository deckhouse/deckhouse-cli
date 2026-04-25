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

package registry

import (
	"context"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

func TestNew_HasDefaults(t *testing.T) {
	o := New()
	if o.Keychain == nil {
		t.Errorf("New() should seed a default keychain")
	}
	if o.Context == nil {
		t.Errorf("New() should seed a background context")
	}
	if o.Platform != nil {
		t.Errorf("New() should leave Platform nil; got %+v", o.Platform)
	}
	// Keychain / platform / context are finalized lazily by remoteWithContext;
	// New() must not pre-bake them into o.Remote, otherwise repeat builder
	// calls would silently stack duplicate upstream options.
	if len(o.Remote) != 0 {
		t.Errorf("New() must leave Remote empty; got %d entries", len(o.Remote))
	}
}

func TestWithContext_ReplacesCtx(t *testing.T) {
	o := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	o.WithContext(ctx)
	if o.Context != ctx {
		t.Errorf("WithContext did not replace Context")
	}
}

func TestWithContext_DoesNotMutateRemote(t *testing.T) {
	// remote.WithContext is produced exclusively by remoteWithContext at fetch
	// time. Pre-baking it into o.Remote would stack a second WithContext when
	// callers pass a derived ctx, relying on go-containerregistry's last-wins
	// semantics - fragile. Guard the contract here.
	o := New()
	before := len(o.Remote)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.WithContext(ctx)
	if len(o.Remote) != before {
		t.Errorf("WithContext should not append to o.Remote; len before=%d after=%d", before, len(o.Remote))
	}
}

func TestRemoteWithContext_FinalizesLazily(t *testing.T) {
	// Default Options + ctx => keychain + ctx are appended at finalize time;
	// o.Remote stays empty (it gets stuff only via WithTransport / WithNondistributable).
	o := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := o.remoteWithContext(ctx)
	if len(got) != 2 {
		t.Errorf("expected 2 finalized options (keychain + ctx); got %d", len(got))
	}
	if len(o.Remote) != 0 {
		t.Errorf("remoteWithContext mutated o.Remote: got len=%d", len(o.Remote))
	}
	// Calling again must not stack more options - finalize is pure of o.Remote.
	got2 := o.remoteWithContext(ctx)
	if len(got2) != len(got) {
		t.Errorf("second call to remoteWithContext returned different length: %d vs %d", len(got2), len(got))
	}
}

func TestWithPlatform_NilIsNoop(t *testing.T) {
	o := New()
	before := len(o.Remote)
	o.WithPlatform(nil)
	if o.Platform != nil {
		t.Errorf("nil platform should not be stored; got %+v", o.Platform)
	}
	if len(o.Remote) != before {
		t.Errorf("nil platform should not append remote options")
	}
}

func TestWithPlatform_Stores(t *testing.T) {
	o := New()
	p, err := v1.ParsePlatform("linux/arm64")
	if err != nil {
		t.Fatalf("ParsePlatform: %v", err)
	}
	o.WithPlatform(p)
	if o.Platform == nil || o.Platform.OS != "linux" || o.Platform.Architecture != "arm64" {
		t.Errorf("Platform not stored: %+v", o.Platform)
	}
}

func TestChainableBuilders(t *testing.T) {
	ctx := context.Background()
	o := New().WithContext(ctx).WithInsecure().WithNondistributable()

	if o.Context != ctx {
		t.Errorf("WithContext did not propagate")
	}
	if len(o.Name) == 0 {
		t.Errorf("WithInsecure should have appended a name.Option")
	}
}

// stubKeychain is a sentinel implementation - we only need pointer identity
// for the anti-duplication tests below.
type stubKeychain struct{ tag string }

func (stubKeychain) Resolve(_ authn.Resource) (authn.Authenticator, error) {
	return authn.Anonymous, nil
}

func TestWithKeychain_LastWriteReplaces(t *testing.T) {
	custom := stubKeychain{tag: "custom"}
	o := New().WithKeychain(custom)
	if _, ok := o.Keychain.(stubKeychain); !ok {
		t.Errorf("Keychain not replaced; got %T", o.Keychain)
	}
	// Pre-fix behaviour appended a second WithAuthFromKeychain to o.Remote;
	// finalize-on-read makes that impossible by construction.
	if len(o.Remote) != 0 {
		t.Errorf("WithKeychain must not stack options on o.Remote; got %d", len(o.Remote))
	}
	// Finalized output must still carry exactly one keychain option (no dupes
	// across repeated finalize calls) plus a ctx.
	got := o.remoteWithContext(context.Background())
	if len(got) != 2 {
		t.Errorf("expected 2 finalized options (keychain + ctx), got %d", len(got))
	}
}

func TestWithPlatform_RepeatedCallsDoNotStack(t *testing.T) {
	p1, _ := v1.ParsePlatform("linux/amd64")
	p2, _ := v1.ParsePlatform("linux/arm64")
	o := New().WithPlatform(p1).WithPlatform(p2)

	if o.Platform == nil || o.Platform.Architecture != "arm64" {
		t.Errorf("last WithPlatform must win; got %+v", o.Platform)
	}
	if len(o.Remote) != 0 {
		t.Errorf("WithPlatform must not stack options on o.Remote; got %d", len(o.Remote))
	}
	got := o.remoteWithContext(context.Background())
	// keychain + platform + ctx
	if len(got) != 3 {
		t.Errorf("expected 3 finalized options (keychain + platform + ctx), got %d", len(got))
	}
}

func TestInsecureTransport_Cloned(t *testing.T) {
	t1 := InsecureTransport()
	t2 := InsecureTransport()
	if t1 == nil || t2 == nil {
		t.Fatalf("InsecureTransport returned nil")
	}
	if t1 == t2 {
		t.Errorf("InsecureTransport should return distinct clones, got the same instance")
	}
}
