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

// ListTags returns every tag of repoRef.
//
// The registry API serves tags in pages (`?n=` plus a `Link: rel="next"`
// cursor), but that is a wire detail with no console equivalent: there is no
// way for a user to ask for "the next range", so callers always get the
// complete list or an error - never a page, and never a truncated list.
// Pages are walked internally by walkTagPages. There is deliberately no cap:
// a partial tag list is indistinguishable from a complete one at the call
// site, which is exactly the failure mode this package avoids.
func ListTags(ctx context.Context, repoRef string, opts *Options) ([]string, error) {
	repo, err := name.NewRepository(repoRef, opts.Name...)
	if err != nil {
		return nil, fmt.Errorf("parse repository %q: %w", repoRef, err)
	}

	lister, err := newTagLister(ctx, repo, opts)
	if err != nil {
		return nil, err
	}

	return walkTagPages(ctx, lister, repo)
}

// newTagLister opens a tag listing, retrying once without the `n` query
// parameter.
//
// go-containerregistry sends `n=1000` by default to ask for large pages. Some
// registries reject an `n` they do not implement (400/UNSUPPORTED) instead of
// ignoring it, which would make `d8 cr ls` unusable against them for no good
// reason: dropping `n` costs only extra round trips, since the cursor walk
// collects the full list either way. remote.WithPageSize(0) omits the
// parameter entirely.
func newTagLister(ctx context.Context, repo name.Repository, opts *Options) (*remote.Lister, error) {
	lister, err := tagLister(ctx, repo, opts.remoteWithContext(ctx))
	if err == nil {
		return lister, nil
	}

	lister, retryErr := tagLister(ctx, repo, append(opts.remoteWithContext(ctx), remote.WithPageSize(0)))
	if retryErr != nil {
		// Surface the original failure: the retry is a compatibility
		// workaround, so its error (an identical 401, most of the time) is
		// rarely the more informative of the two.
		return nil, fmt.Errorf("read tags for %s: %w", repo, err)
	}

	return lister, nil
}

func tagLister(ctx context.Context, repo name.Repository, remoteOpts []remote.Option) (*remote.Lister, error) {
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, fmt.Errorf("create puller: %w", err)
	}

	return puller.Lister(ctx, repo)
}

// walkTagPages concatenates every page into one slice.
//
// A registry that echoes the same `Link: rel="next"` cursor on every response
// (i.e. one that ignores `last=`) keeps HasNext true forever, so the walk
// would spin and accumulate duplicates until the user interrupted it.
// Refusing a cursor we have already followed turns that into a clean error;
// a repeated cursor can never make progress, so this cannot reject a
// legitimate listing.
func walkTagPages(ctx context.Context, lister *remote.Lister, repo name.Repository) ([]string, error) {
	var out []string

	seen := make(map[string]struct{})

	for lister.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		page, err := lister.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("read tags for %s: %w", repo, err)
		}

		out = append(out, page.Tags...)

		if page.Next == "" {
			break
		}

		if _, dup := seen[page.Next]; dup {
			return nil, fmt.Errorf("read tags for %s: registry keeps returning the same pagination cursor %q, refusing to loop", repo, page.Next)
		}

		seen[page.Next] = struct{}{}
	}

	return out, nil
}
