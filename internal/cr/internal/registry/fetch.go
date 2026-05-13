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
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// Fetch resolves ref and returns a v1.Image. For multi-arch indices
// remote.Image picks the current runtime platform unless opts.Platform pins
// another one.
func Fetch(ctx context.Context, ref string, opts *Options) (v1.Image, error) {
	parsed, err := name.ParseReference(ref, opts.Name...)
	if err != nil {
		return nil, fmt.Errorf("parse reference %q: %w", ref, err)
	}
	img, err := remote.Image(parsed, opts.remoteWithContext(ctx)...)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", ref, err)
	}
	return img, nil
}

// FetchDescriptor returns the raw remote descriptor, leaving media-type
// dispatch to the caller (pull uses it to tell an index from an image).
func FetchDescriptor(ctx context.Context, ref string, opts *Options) (*remote.Descriptor, error) {
	parsed, err := name.ParseReference(ref, opts.Name...)
	if err != nil {
		return nil, fmt.Errorf("parse reference %q: %w", ref, err)
	}
	desc, err := remote.Get(parsed, opts.remoteWithContext(ctx)...)
	if err != nil {
		return nil, fmt.Errorf("fetch descriptor %s: %w", ref, err)
	}
	return desc, nil
}

// remoteWithContext is the single point where Options is converted to a
// []remote.Option. Keychain / platform / context are finalized here so that
// repeated builder calls (e.g. WithPlatform twice) cannot stack duplicate
// upstream options on o.Remote and rely on go-containerregistry's
// last-write-wins semantics. o.Remote stays untouched, so the same Options
// can be used to dispatch calls with different per-call contexts.
func (o *Options) remoteWithContext(ctx context.Context) []remote.Option {
	if ctx == nil {
		ctx = o.Context
	}
	out := make([]remote.Option, 0, len(o.Remote)+3)
	out = append(out, o.Remote...)
	if o.Keychain != nil {
		out = append(out, remote.WithAuthFromKeychain(o.Keychain))
	}
	if o.Platform != nil {
		out = append(out, remote.WithPlatform(*o.Platform))
	}
	if ctx != nil {
		out = append(out, remote.WithContext(ctx))
	}
	return out
}
