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

// FetchManifest returns the raw manifest bytes as the registry served them,
// which is what signature verification and audit trails need - a manifest
// decoded and re-encoded no longer hashes to its own digest.
//
// With a platform pinned, a multi-arch reference resolves to that child's
// manifest instead of the index. Without one the index is returned as served.
func FetchManifest(ctx context.Context, ref string, opts *Options) ([]byte, error) {
	client, id, err := clientForRef(ref, opts)
	if err != nil {
		return nil, err
	}

	res, err := client.GetManifest(ctx, id, opts.manifestGetOptions()...)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest %s: %w", ref, err)
	}

	return res.GetRaw(), nil
}

// FetchConfig returns the raw config JSON for ref, byte-for-byte as stored, so
// it stays pipeable into jq and comparable across pulls.
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

// FetchDigest returns "sha256:<hex>" for ref.
//
// With a platform pinned this is the digest of that child image, not of the
// index - the whole point of asking for a digest is to pin what will actually
// run, and an index digest does not identify a single image.
func FetchDigest(ctx context.Context, ref string, opts *Options) (string, error) {
	client, id, err := clientForRef(ref, opts)
	if err != nil {
		return "", err
	}

	res, err := client.GetManifest(ctx, id, opts.manifestGetOptions()...)
	if err != nil {
		return "", fmt.Errorf("fetch digest %s: %w", ref, err)
	}

	desc := res.GetDescriptor()
	if desc == nil {
		return "", fmt.Errorf("fetch digest %s: registry returned a manifest without a descriptor", ref)
	}

	return desc.GetDigest().String(), nil
}
