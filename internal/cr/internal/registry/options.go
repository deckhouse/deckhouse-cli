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
	"io"
	"log/slog"
	"os"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	dkpclient "github.com/deckhouse/deckhouse/pkg/registry/client"
)

// Options carries the `d8 cr` persistent flags and turns them into a
// deckhouse/pkg/registry client scoped to whatever reference a command names.
//
// The flags live here rather than in a client instance because `d8 cr` is
// reference-oriented - every command takes a full "host/repo:tag" argument -
// while the client is scoped to one repository. clientForRef bridges the two.
type Options struct {
	// PlainHTTP talks to the registry over HTTP instead of HTTPS.
	PlainHTTP bool
	// TLSSkipVerify accepts any server certificate.
	TLSSkipVerify bool
	// Nondistributable uploads foreign layers on push instead of skipping them.
	Nondistributable bool
	// Verbose routes the client's debug log to stderr.
	Verbose bool

	// Platform pins a target platform for multi-arch indices. Nil means the
	// reference is used as served, except where a command must resolve to a
	// single image - see Fetch.
	Platform *v1.Platform

	// Auth authenticates every request when the user passed --username/--password.
	// It takes precedence over Keychain, and being per-client it cannot leak
	// credentials to a registry the user did not name.
	Auth authn.Authenticator
	// Keychain resolves credentials from the Docker config when Auth is unset.
	Keychain authn.Keychain

	// ClientFactory builds the registry client for one host. Nil means the real
	// deckhouse/pkg/registry client.
	//
	// Tests set it to drive this package against pkg/registry's in-memory fake
	// and stay off the network entirely: standing up an HTTP registry per test
	// both slows the suite down and produces timeout flake under parallel
	// `go test ./...` load. It is a field rather than a package-level hook so
	// tests can run in parallel without fighting over global state.
	ClientFactory func(host string, opts ...dkpclient.Option) dkpreg.Client
}

// New returns Options seeded with the default Docker keychain, so every command
// authenticates from ~/.docker/config.json without a prior `cr login`.
func New() *Options {
	return &Options{Keychain: authn.DefaultKeychain}
}

// WithKeychain replaces the keychain that authenticates registry calls.
func (o *Options) WithKeychain(kc authn.Keychain) *Options {
	o.Keychain = kc
	return o
}

// WithAuth pins explicit credentials, as --username/--password do.
func (o *Options) WithAuth(auth authn.Authenticator) *Options {
	o.Auth = auth
	return o
}

// WithPlatform pins a target platform. Nil is a no-op so a flag-driven caller
// can pass the parsed result without branching.
func (o *Options) WithPlatform(p *v1.Platform) *Options {
	if p == nil {
		return o
	}

	o.Platform = p

	return o
}

// WithInsecure opts into plain HTTP and accepts any TLS certificate.
//
// The two are separate on the client (WithInsecure / WithTLSSkipVerify) and
// deserve separate flags here too, but `--insecure` has always meant both, so
// the flag keeps setting both until it is split.
func (o *Options) WithInsecure() *Options {
	o.PlainHTTP = true
	o.TLSSkipVerify = true

	return o
}

// WithNondistributable allows pushing foreign (non-distributable) layers.
func (o *Options) WithNondistributable() *Options {
	o.Nondistributable = true
	return o
}

// WithVerbose enables the client's debug log on stderr.
func (o *Options) WithVerbose() *Options {
	o.Verbose = true
	return o
}

// nameOptions are the go-containerregistry parsing flags implied by Options.
// Reference parsing stays here because `d8 cr` has to split a user-supplied
// reference into registry, repository and tag before it can build a client.
func (o *Options) nameOptions() []name.Option {
	if o.PlainHTTP {
		return []name.Option{name.Insecure}
	}

	return nil
}

// clientOptions translates the flags into client options.
func (o *Options) clientOptions() []dkpclient.Option {
	opts := []dkpclient.Option{
		dkpclient.WithInsecure(o.PlainHTTP),
		dkpclient.WithTLSSkipVerify(o.TLSSkipVerify),
		dkpclient.WithLogger(o.logger()),
	}

	// Auth wins over the keychain, matching the client's own precedence.
	if o.Auth != nil {
		return append(opts, dkpclient.WithAuth(o.Auth))
	}

	if o.Keychain != nil {
		return append(opts, dkpclient.WithKeychain(o.Keychain))
	}

	return opts
}

// logger keeps the client's log off stdout.
//
// The client logs at debug on every operation and its default logger writes to
// stdout, which would corrupt the one thing several commands exist to produce:
// "d8 cr manifest ref | jq" and "d8 cr export ref - | tar tf -" both put bytes
// on stdout that must stay pristine. So the sink is always stderr, and the
// level is silent unless -v asked for it.
func (o *Options) logger() *dkplog.Logger {
	if o.Verbose {
		return dkplog.NewLogger(
			dkplog.WithLevel(slog.LevelDebug),
			dkplog.WithOutput(os.Stderr),
		).Named("cr")
	}

	return dkplog.NewLogger(
		dkplog.WithLevel(slog.LevelError),
		dkplog.WithOutput(io.Discard),
	).Named("cr")
}
