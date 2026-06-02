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
// An empty host targets Docker Hub. Verification performs the standard
// registry auth handshake (ping + token exchange); invalid credentials fail
// here rather than silently writing a broken config.
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

	// Empty scope keeps this a registry-level check: it pings /v2/ and, on a
	// bearer challenge, exchanges the basic credentials for a token. Wrong
	// credentials surface as a 401 here. A repository/catalog scope would
	// additionally demand permissions a plain login user need not have.
	if _, err := transport.NewWithContext(ctx, reg, auth, rt, []string{}); err != nil {
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
