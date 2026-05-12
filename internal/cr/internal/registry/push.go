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
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// Push writes obj (v1.Image or v1.ImageIndex) under ref and returns the
// resulting digest. Anything else is a programmer error.
func Push(ctx context.Context, ref string, obj partial.WithRawManifest, opts *Options) (v1.Hash, error) {
	// A literal-nil and a typed-nil v1.Image/v1.ImageIndex both land here
	// as a nil interface (since v1.Image and v1.ImageIndex are themselves
	// interfaces). Catching it up front keeps the type switch from doing
	// remote.Write on a nil object and panicking inside go-containerregistry.
	if obj == nil {
		return v1.Hash{}, fmt.Errorf("push %s: object is nil", ref)
	}
	parsed, err := name.ParseReference(ref, opts.Name...)
	if err != nil {
		return v1.Hash{}, fmt.Errorf("parse reference %q: %w", ref, err)
	}
	remoteOpts := opts.remoteWithContext(ctx)
	switch t := obj.(type) {
	case v1.Image:
		if err := remote.Write(parsed, t, remoteOpts...); err != nil {
			return v1.Hash{}, fmt.Errorf("push image %s: %w", ref, err)
		}
		return t.Digest()
	case v1.ImageIndex:
		if err := remote.WriteIndex(parsed, t, remoteOpts...); err != nil {
			return v1.Hash{}, fmt.Errorf("push index %s: %w", ref, err)
		}
		return t.Digest()
	default:
		return v1.Hash{}, fmt.Errorf("push %s: unsupported type %T", ref, obj)
	}
}
