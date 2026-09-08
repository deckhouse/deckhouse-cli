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

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// ListCatalog returns every repository on the given registry. Like ListTags,
// it hides the page-by-page wire protocol: callers get the complete list or
// an error, never a truncated list; see ListTags.
//
// Not every registry implements /v2/_catalog - Docker Hub and GCR/GAR do not -
// and the underlying call surfaces that as a 404 through the error chain.
func ListCatalog(ctx context.Context, regRef string, opts *Options) ([]string, error) {
	reg, err := name.NewRegistry(regRef, opts.Name...)
	if err != nil {
		return nil, fmt.Errorf("parse registry %q: %w", regRef, err)
	}

	catalogger, err := newCatalogger(ctx, reg, opts)
	if err != nil {
		return nil, err
	}

	return walkCatalogPages(ctx, catalogger, reg)
}

// newCatalogger mirrors newTagLister, including the retry that drops the `n`
// query parameter for registries that reject it.
func newCatalogger(ctx context.Context, reg name.Registry, opts *Options) (*remote.Catalogger, error) {
	catalogger, err := openCatalogger(ctx, reg, opts.remoteWithContext(ctx))
	if err == nil {
		return catalogger, nil
	}

	catalogger, retryErr := openCatalogger(ctx, reg, append(opts.remoteWithContext(ctx), remote.WithPageSize(0)))
	if retryErr != nil {
		return nil, fmt.Errorf("read catalog for %s: %w", reg, err)
	}

	return catalogger, nil
}

func openCatalogger(ctx context.Context, reg name.Registry, remoteOpts []remote.Option) (*remote.Catalogger, error) {
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, fmt.Errorf("create puller: %w", err)
	}

	return puller.Catalogger(ctx, reg)
}

// walkCatalogPages is the ListCatalog counterpart of walkTagPages; see there
// for why a repeated cursor is refused.
func walkCatalogPages(ctx context.Context, catalogger *remote.Catalogger, reg name.Registry) ([]string, error) {
	var out []string

	seen := make(map[string]struct{})

	for catalogger.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		page, err := catalogger.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("read catalog for %s: %w", reg, err)
		}

		out = append(out, page.Repos...)

		if page.Next == "" {
			break
		}

		if _, dup := seen[page.Next]; dup {
			return nil, fmt.Errorf("read catalog for %s: registry keeps returning the same pagination cursor %q, refusing to loop", reg, page.Next)
		}

		seen[page.Next] = struct{}{}
	}

	return out, nil
}
