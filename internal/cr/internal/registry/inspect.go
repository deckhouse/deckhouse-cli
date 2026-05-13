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
)

// FetchManifest returns the raw manifest bytes as the registry served them.
// This preserves signatures and byte-for-byte JSON the user may want to pipe.
func FetchManifest(ctx context.Context, ref string, opts *Options) ([]byte, error) {
	desc, err := FetchDescriptor(ctx, ref, opts)
	if err != nil {
		return nil, err
	}
	return desc.Manifest, nil
}

// FetchConfig returns the config JSON for ref. Multi-arch indices are
// resolved via the caller's platform (set on Options).
func FetchConfig(ctx context.Context, ref string, opts *Options) ([]byte, error) {
	img, err := Fetch(ctx, ref, opts)
	if err != nil {
		return nil, err
	}
	cfg, err := img.RawConfigFile()
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", ref, err)
	}
	return cfg, nil
}

// FetchDigest returns "sha256:<hex>" for ref's manifest as served.
func FetchDigest(ctx context.Context, ref string, opts *Options) (string, error) {
	desc, err := FetchDescriptor(ctx, ref, opts)
	if err != nil {
		return "", err
	}
	return desc.Digest.String(), nil
}
