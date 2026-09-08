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
	"errors"
	"fmt"

	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
)

// ListCatalog returns every repository on the given registry.
//
// Registries that do not implement /v2/_catalog - Docker Hub, GCR and Artifact
// Registry among them - are reported as such rather than as a bare 404, which
// otherwise reads like a missing repository and sends users hunting for a
// permissions problem they do not have.
func ListCatalog(ctx context.Context, regRef string, opts *Options) ([]string, error) {
	client, err := clientForRegistryRef(regRef, opts)
	if err != nil {
		return nil, err
	}

	repos, err := client.ListRepositories(ctx)
	if err != nil {
		if errors.Is(err, dkpreg.ErrCatalogNotSupported) {
			return nil, fmt.Errorf("%s does not support listing repositories (no /v2/_catalog): %w", regRef, err)
		}

		return nil, fmt.Errorf("read catalog for %s: %w", regRef, err)
	}

	return repos, nil
}
