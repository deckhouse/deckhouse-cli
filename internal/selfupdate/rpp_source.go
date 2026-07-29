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

	"github.com/deckhouse/deckhouse-cli/internal/rpp"
)

// cliBinaryEntryName is the file inside the deckhouse-cli image that holds the d8
// executable (verified against the published image).
const cliBinaryEntryName = "d8"

// rppSource extracts deckhouse-cli releases through the registry-packages-proxy.
//
// Releases are published as multi-platform image indexes under plain version tags
// ("v1.2.3"). The proxy selects the current platform's image from the index when
// PullImage sends its ?platform= query, so tags are plain versions here.
type rppSource struct {
	client *rpp.Client
}

var _ Source = (*rppSource)(nil)

// NewRPPSource builds the proxy-backed release source.
func NewRPPSource(client *rpp.Client) Source {
	return &rppSource{client: client}
}

// ListTags returns the available release tags. Tags are plain version strings;
// the proxy selects the per-platform image at pull time (PullImage's ?platform=
// query), so there is nothing to normalize here.
func (s *rppSource) ListTags(ctx context.Context) ([]string, error) {
	return s.client.ListTags(ctx, rpp.CLIImage())
}

// ExtractBinary downloads the d8 binary for tag. The proxy resolves the current
// platform's image from the tag's multi-platform index (PullImage attaches the
// ?platform= query).
func (s *rppSource) ExtractBinary(ctx context.Context, tag, destination string) error {
	body, err := s.client.PullImage(ctx, rpp.CLIImage(), tag)
	if err != nil {
		return err
	}

	defer func() { _ = body.Close() }()

	return rpp.ExtractFileToPath(body, cliBinaryEntryName, destination, rpp.ExecutableMode, rpp.DefaultBinaryByteLimit)
}
