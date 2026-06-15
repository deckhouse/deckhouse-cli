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

package selfupdate

import (
	"context"
	"errors"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/rpp"
)

// cliBinaryEntryName is the file inside the deckhouse-cli image that holds the d8
// executable (verified against the published image).
const cliBinaryEntryName = "d8"

// rppSource extracts deckhouse-cli releases through the registry-packages-proxy.
//
// Tags may be published per platform ("v1.2.3-linux-amd64", one single-platform
// image per tag - the same convention the plugin CI uses). The source hides that
// from the Updater: ListTags reports such tags as their bare version, and
// ExtractBinary resolves the bare version back to the platform tag, falling back
// to the bare tag itself for platform-neutral/legacy publishing.
type rppSource struct {
	client *rpp.Client
}

var _ Source = (*rppSource)(nil)

// NewRPPSource builds the proxy-backed release source.
func NewRPPSource(client *rpp.Client) Source {
	return &rppSource{client: client}
}

// ListTags returns the available release tags normalized for the current
// platform: "v1.2.3-<os>-<arch>" of THIS platform becomes "v1.2.3", tags of other
// platforms pass through raw (their suffix parses as a semver pre-release, so the
// Updater never auto-selects them).
func (s *rppSource) ListTags(ctx context.Context) ([]string, error) {
	tags, err := s.client.ListTags(ctx, rpp.CLIImage())
	if err != nil {
		return nil, err
	}

	suffix := rpp.PlatformSuffix()
	seen := make(map[string]struct{}, len(tags))
	normalized := make([]string, 0, len(tags))

	for _, tag := range tags {
		tag = strings.TrimSuffix(tag, suffix)
		if _, ok := seen[tag]; ok {
			continue
		}

		seen[tag] = struct{}{}
		normalized = append(normalized, tag)
	}

	return normalized, nil
}

// ExtractBinary downloads the d8 binary for tag, preferring the per-platform tag
// ("<tag>-<os>-<arch>") and falling back to the bare tag when the platform tag is
// not published.
func (s *rppSource) ExtractBinary(ctx context.Context, tag, destination string) error {
	err := s.extract(ctx, tag+rpp.PlatformSuffix(), destination)
	if errors.Is(err, rpp.ErrNotFound) {
		return s.extract(ctx, tag, destination)
	}

	return err
}

func (s *rppSource) extract(ctx context.Context, tag, destination string) error {
	body, err := s.client.PullImage(ctx, rpp.CLIImage(), tag)
	if err != nil {
		return err
	}

	defer func() { _ = body.Close() }()

	return rpp.ExtractFileToPath(body, cliBinaryEntryName, destination, rpp.ExecutableMode, rpp.DefaultBinaryByteLimit)
}
