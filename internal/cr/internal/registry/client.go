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
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"

	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	dkpclient "github.com/deckhouse/deckhouse/pkg/registry/client"
)

// ParseReference parses a user-supplied image reference under the flags on
// Options - notably --insecure, which permits a plain-HTTP registry.
//
// Commands need this to echo a canonical reference back to the user (push
// prints the pushed digest reference, ls --full-ref prefixes every tag), and
// routing it through here keeps one set of parsing flags for the whole subtree.
func ParseReference(ref string, opts *Options) (name.Reference, error) {
	parsed, err := name.ParseReference(ref, opts.nameOptions()...)
	if err != nil {
		return nil, fmt.Errorf("parse reference %q: %w", ref, err)
	}

	return parsed, nil
}

// ParseRepository is ParseReference for arguments that name a repository.
func ParseRepository(repoRef string, opts *Options) (name.Repository, error) {
	repo, err := name.NewRepository(repoRef, opts.nameOptions()...)
	if err != nil {
		return name.Repository{}, fmt.Errorf("parse repository %q: %w", repoRef, err)
	}

	return repo, nil
}

// clientForRef parses a user-supplied image reference and returns a client
// scoped to its repository, plus the identifier (tag or "sha256:...") that
// addresses one manifest inside it.
//
// This is the whole adapter between the two shapes: `d8 cr` arguments are full
// references, while dkpreg.Client is scoped by host plus path segments. Parsing
// stays on this side because only the CLI knows the reference is user input
// that has to be validated before anything touches the network.
func clientForRef(ref string, opts *Options) (dkpreg.Client, string, error) {
	parsed, err := name.ParseReference(ref, opts.nameOptions()...)
	if err != nil {
		return nil, "", fmt.Errorf("parse reference %q: %w", ref, err)
	}

	repo := parsed.Context()

	return clientForRepository(repo, opts), parsed.Identifier(), nil
}

// clientForRepoRef is clientForRef for arguments that name a repository and
// never a tag, such as `cr ls REPO`.
func clientForRepoRef(repoRef string, opts *Options) (dkpreg.Client, error) {
	repo, err := name.NewRepository(repoRef, opts.nameOptions()...)
	if err != nil {
		return nil, fmt.Errorf("parse repository %q: %w", repoRef, err)
	}

	return clientForRepository(repo, opts), nil
}

// clientForRegistryRef is clientForRef for arguments that name a registry, such
// as `cr catalog REGISTRY`.
func clientForRegistryRef(regRef string, opts *Options) (dkpreg.Client, error) {
	reg, err := name.NewRegistry(regRef, opts.nameOptions()...)
	if err != nil {
		return nil, fmt.Errorf("parse registry %q: %w", regRef, err)
	}

	return opts.newClient(reg.RegistryStr()), nil
}

// newClient builds the client for one host, through the test seam when set.
func (o *Options) newClient(host string) dkpreg.Client {
	if o.ClientFactory != nil {
		return o.ClientFactory(host, o.clientOptions()...)
	}

	return dkpclient.New(host, o.clientOptions()...)
}

func clientForRepository(repo name.Repository, opts *Options) dkpreg.Client {
	client := opts.newClient(repo.RegistryStr())

	// A bare registry reference has no repository path; WithSegment("") would
	// otherwise append an empty segment and address "host/".
	if path := repo.RepositoryStr(); path != "" {
		return client.WithSegment(strings.Split(path, "/")...)
	}

	return client
}

// pushOptions are the client push options implied by Options.
func (o *Options) pushOptions() []dkpreg.ImagePushOption {
	if o.Nondistributable {
		return []dkpreg.ImagePushOption{dkpclient.WithNondistributable()}
	}

	return nil
}

// imageGetOptions are the client image-get options implied by Options.
func (o *Options) imageGetOptions() []dkpreg.ImageGetOption {
	if o.Platform != nil {
		return []dkpreg.ImageGetOption{dkpclient.WithPlatform{Platform: o.Platform}}
	}

	return nil
}

// manifestGetOptions are the client manifest-get options implied by Options.
func (o *Options) manifestGetOptions() []dkpreg.ManifestGetOption {
	if o.Platform != nil {
		return []dkpreg.ManifestGetOption{dkpclient.WithPlatform{Platform: o.Platform}}
	}

	return nil
}
