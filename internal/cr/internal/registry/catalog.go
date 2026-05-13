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

// ListCatalog invokes visit for every repository page on the given registry.
// Not every registry implements /v2/_catalog - the underlying call will
// surface a 404 through the error chain.
func ListCatalog(ctx context.Context, regRef string, opts *Options, visit func(repos []string) error) error {
	reg, err := name.NewRegistry(regRef, opts.Name...)
	if err != nil {
		return fmt.Errorf("parse registry %q: %w", regRef, err)
	}

	puller, err := remote.NewPuller(opts.remoteWithContext(ctx)...)
	if err != nil {
		return fmt.Errorf("create puller: %w", err)
	}

	catalogger, err := puller.Catalogger(ctx, reg)
	if err != nil {
		return fmt.Errorf("read catalog for %s: %w", reg, err)
	}

	for catalogger.HasNext() {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := catalogger.Next(ctx)
		if err != nil {
			return fmt.Errorf("read next catalog page: %w", err)
		}
		if err := visit(page.Repos); err != nil {
			return err
		}
	}
	return nil
}
