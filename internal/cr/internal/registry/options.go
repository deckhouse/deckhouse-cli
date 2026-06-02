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
	"net/http"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// Options accumulates everything the domain layer needs to talk to a
// registry: auth, transport, platform hint, name-parsing flags. Each builder
// mutates the receiver and returns it so calls chain.
//
// The two slices (Remote, Name) are what actually gets passed to
// go-containerregistry: Remote to remote.*, Name to name.ParseReference /
// name.NewRepository / name.NewRegistry / name.NewTag.
type Options struct {
	Remote   []remote.Option
	Name     []name.Option
	Platform *v1.Platform
	Keychain authn.Keychain
	Context  context.Context

	// Transport mirrors the http.RoundTripper installed via WithTransport.
	// remote.* receives it through o.Remote; we also keep a direct handle so
	// auth-only flows (e.g. Login) that talk to the registry transport
	// package can honour --insecure without re-deriving it from o.Remote.
	Transport http.RoundTripper
}

// New returns Options seeded with the default Docker keychain and a
// background context. Keychain / platform / context are NOT baked into
// o.Remote here - they are finalized lazily by remoteWithContext at fetch
// time so repeated builder calls (e.g. WithPlatform twice with different
// values) cannot stack duplicate options on the slice.
func New() *Options {
	return &Options{
		Keychain: authn.DefaultKeychain,
		Context:  context.Background(),
	}
}

// WithContext replaces the ambient context.
func (o *Options) WithContext(ctx context.Context) *Options {
	o.Context = ctx
	return o
}

// WithKeychain replaces the keychain that authenticates registry calls.
// Last call wins.
func (o *Options) WithKeychain(kc authn.Keychain) *Options {
	o.Keychain = kc
	return o
}

// WithPlatform pins a target platform for multi-arch indices. Nil is a no-op
// (so a flag-driven caller can pass the parsed result directly without
// branching). Last non-nil call wins.
func (o *Options) WithPlatform(p *v1.Platform) *Options {
	if p == nil {
		return o
	}

	o.Platform = p

	return o
}

// WithInsecure tolerates non-TLS references during name parsing. The HTTP
// transport itself is configured separately via WithTransport.
func (o *Options) WithInsecure() *Options {
	o.Name = append(o.Name, name.Insecure)
	return o
}

// WithNondistributable allows pushing foreign (non-distributable) layers.
func (o *Options) WithNondistributable() *Options {
	o.Remote = append(o.Remote, remote.WithNondistributable)
	return o
}

// WithTransport installs a custom HTTP transport (typically a clone of
// remote.DefaultTransport with TLS skip-verify toggled).
func (o *Options) WithTransport(t http.RoundTripper) *Options {
	o.Transport = t
	o.Remote = append(o.Remote, remote.WithTransport(t))
	return o
}

// staticKeychain authenticates every registry with a single set of
// credentials. Used when the user passes --username/--password so all cr
// commands can talk to the registry without a prior `cr login`.
type staticKeychain struct {
	auth authn.Authenticator
}

func (k staticKeychain) Resolve(authn.Resource) (authn.Authenticator, error) {
	return k.auth, nil
}

// NewStaticKeychain returns a keychain that always resolves to the given
// username/password, regardless of the target registry.
func NewStaticKeychain(username, password string) authn.Keychain {
	return staticKeychain{
		auth: authn.FromConfig(authn.AuthConfig{
			Username: username,
			Password: password,
		}),
	}
}
