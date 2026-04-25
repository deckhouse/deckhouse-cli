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

// ListTags invokes visit for every tag page of repo. Stopping early: return
// a non-nil error (context.Canceled is reasonable for user abort).
func ListTags(ctx context.Context, repoRef string, opts *Options, visit func(tags []string) error) error {
	repo, err := name.NewRepository(repoRef, opts.Name...)
	if err != nil {
		return fmt.Errorf("parse repository %q: %w", repoRef, err)
	}

	puller, err := remote.NewPuller(opts.remoteWithContext(ctx)...)
	if err != nil {
		return fmt.Errorf("create puller: %w", err)
	}

	lister, err := puller.Lister(ctx, repo)
	if err != nil {
		return fmt.Errorf("read tags for %s: %w", repo, err)
	}

	for lister.HasNext() {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := lister.Next(ctx)
		if err != nil {
			return fmt.Errorf("read next tag page: %w", err)
		}
		if err := visit(page.Tags); err != nil {
			return err
		}
	}
	return nil
}
