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
	"fmt"
	"net/http"

	"github.com/docker/cli/cli/config"
	"github.com/docker/cli/cli/config/types"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

// LoginResult reports where the credentials ended up so the caller can tell
// the user which file/store now holds them.
type LoginResult struct {
	// ServerAddress is the key the credentials were stored under. For Docker
	// Hub this is the canonical "https://index.docker.io/v1/".
	ServerAddress string
	// ConfigFile is the path to the Docker config the credentials were
	// written to (or the backing file of the active credential store).
	ConfigFile string
}

// Login verifies username/password against host and, on success, persists
// them into the Docker config (the same store authn.DefaultKeychain reads),
// so every other cr command can authenticate transparently afterwards.
//
// An empty host targets Docker Hub. Verification builds the registry's auth
// transport and then performs an authenticated GET /v2/: a rejected login
// answers 401 there, so invalid credentials fail rather than silently writing
// a broken config. (The transport handshake alone only validates bearer-token
// registries; the explicit /v2/ probe is what also covers basic-auth ones.)
func Login(ctx context.Context, host, username, password string, opts *Options) (*LoginResult, error) {
	if host == "" {
		host = name.DefaultRegistry
	}

	reg, err := name.NewRegistry(host, opts.Name...)
	if err != nil {
		return nil, fmt.Errorf("parse registry %q: %w", host, err)
	}

	auth := authn.FromConfig(authn.AuthConfig{Username: username, Password: password})

	rt := opts.Transport
	if rt == nil {
		rt = remote.DefaultTransport
	}

	// Build the registry's auth transport. Empty scope keeps this a
	// registry-level check (a repository/catalog scope would demand
	// permissions a plain login user need not have). For a bearer challenge
	// this already runs the token exchange, so wrong credentials fail here;
	// for a basic challenge it only wraps the transport after an anonymous
	// ping and never sends the credentials, so it cannot reject them.
	authRT, err := transport.NewWithContext(ctx, reg, auth, rt, []string{})
	if err != nil {
		return nil, fmt.Errorf("verify credentials for %s: %w", reg.RegistryStr(), err)
	}

	// Actually exercise the credentials with an authenticated GET /v2/. This
	// is what rejects wrong basic-auth credentials, which the handshake above
	// lets through: /v2/ answers 200 to an authenticated client and 401 to a
	// rejected one (exactly how `docker login` validates).
	if err := verifyCredentials(ctx, reg, authRT); err != nil {
		return nil, fmt.Errorf("verify credentials for %s: %w", reg.RegistryStr(), err)
	}

	serverAddress := dockerServerAddress(reg)

	cf, err := config.Load(config.Dir())
	if err != nil {
		return nil, fmt.Errorf("load docker config: %w", err)
	}

	store := cf.GetCredentialsStore(serverAddress)
	if err := store.Store(types.AuthConfig{
		ServerAddress: serverAddress,
		Username:      username,
		Password:      password,
	}); err != nil {
		return nil, fmt.Errorf("store credentials for %s: %w", serverAddress, err)
	}

	return &LoginResult{
		ServerAddress: serverAddress,
		ConfigFile:    cf.GetFilename(),
	}, nil
}

// verifyCredentials performs an authenticated GET /v2/ through rt (the
// transport returned by transport.NewWithContext) and treats anything other
// than 200 OK as failure. The /v2/ base endpoint is the registry API's auth
// probe: it answers 200 to an authenticated client and 401 to a rejected one,
// which makes it the credential check transport.NewWithContext skips for
// basic-auth registries.
func verifyCredentials(ctx context.Context, reg name.Registry, rt http.RoundTripper) error {
	endpoint := fmt.Sprintf("%s://%s/v2/", reg.Scheme(), reg.RegistryStr())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}

	resp, err := (&http.Client{Transport: rt}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return transport.CheckError(resp, http.StatusOK)
}

// dockerServerAddress maps a parsed registry to the key Docker stores
// credentials under. Docker Hub is special-cased to the canonical
// "https://index.docker.io/v1/" so authn.DefaultKeychain (which rewrites the
// default registry to that key on lookup) finds the credentials again.
func dockerServerAddress(reg name.Registry) string {
	if reg.RegistryStr() == name.DefaultRegistry {
		return authn.DefaultAuthKey
	}

	return reg.RegistryStr()
}
