// Copyright 2026 Flant JSC
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	localreg "github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	mlayouts "github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// scopedBundleClient keeps image references scoped to their repository, as the real client does.
type scopedBundleClient struct {
	localreg.Client
	registry string
}

func (c *scopedBundleClient) GetRegistry() string { return c.registry }

func (c *scopedBundleClient) WithSegment(segments ...string) localreg.Client {
	return &scopedBundleClient{
		Client:   c.Client.WithSegment(segments...),
		registry: strings.Join(append([]string{c.registry}, segments...), "/"),
	}
}

func TestPullPlatform_IncludePlatform_RangeBundleChannels(t *testing.T) {
	versions := []string{"v1.74.17", "v1.75.6", "v1.75.16", "v1.76.16", "v1.76.17"}
	selectedVersions := []string{"v1.75.6", "v1.75.16", "v1.76.16"}
	for _, tt := range []struct {
		name             string
		stableVersion    string
		includedChannels []string
	}{
		{
			name:             "range_boundaries",
			stableVersion:    "v1.75.6",
			includedChannels: []string{"stable", "early-access", "beta"},
		},
		{
			name:             "stable_above_range",
			stableVersion:    "v1.76.17",
			includedChannels: []string{"early-access", "beta"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			reg := upfake.NewRegistry(stubRootURL)
			for _, version := range versions {
				img := upfake.NewImageBuilder().
					WithFile("changelog.yaml", "{}").
					WithFile("version.json", fmt.Sprintf(`{"version":%q}`, version)).
					WithFile("deckhouse/candi/images_digests.json", `{}`).MustBuild()
				for _, segment := range []string{"", "install", "install-standalone", "release-channel"} {
					reg.MustAddImage(segment, version, img)
				}
			}
			for channel, version := range map[string]string{
				"rock-solid":   "v1.74.17",
				"stable":       tt.stableVersion,
				"early-access": "v1.75.16",
				"beta":         "v1.76.16",
				"alpha":        "v1.76.17",
			} {
				img := upfake.NewImageBuilder().
					WithFile("changelog.yaml", "{}").
					WithFile("version.json", fmt.Sprintf(`{"version":%q}`, version)).MustBuild()
				reg.MustAddImage("release-channel", channel, img)
			}
			client := &scopedBundleClient{Client: pkgclient.Adapt(upfake.NewClient(reg)), registry: stubRootURL}
			logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
			bundleDir := t.TempDir()
			svc := NewService(registryservice.NewService(client, pkg.NoEdition, logger), t.TempDir(),
				&Options{BundleDir: bundleDir, IncludeConstraint: includePlatformConstraint(t, ">=1.75.6 <=1.76.16")},
				logger, log.NewSLogger(slog.LevelWarn))
			require.NoError(t, svc.PullPlatform(ctx))

			archive, err := os.Open(filepath.Join(bundleDir, "platform.tar"))
			require.NoError(t, err)
			defer archive.Close()
			unpackDir := t.TempDir()
			require.NoError(t, bundle.Unpack(ctx, archive, unpackDir, "platform"))
			releaseChannels := layout.Path(filepath.Join(unpackDir, internal.ReleaseChannelSegment))
			for _, channel := range internal.GetAllDefaultReleaseChannels() {
				descriptor, err := mlayouts.FindImageDescriptorByTag(releaseChannels, channel)
				if !slices.Contains(tt.includedChannels, channel) {
					assert.Errorf(t, err, "channel %s outside the range must be absent", channel)
					continue
				}
				require.NoError(t, err, "channel %s must be included", channel)
				expectedDigest, err := client.WithSegment(internal.ReleaseChannelSegment).GetDigest(ctx, channel)
				require.NoError(t, err)
				assert.Equal(t, expectedDigest.String(), descriptor.Digest.String(), "metadata for channel %s", channel)
			}
			for _, segment := range []string{"", internal.ReleaseChannelSegment} {
				for _, version := range versions {
					_, err := mlayouts.FindImageDescriptorByTag(layout.Path(filepath.Join(unpackDir, segment)), version)
					if slices.Contains(selectedVersions, version) {
						assert.NoError(t, err, "%s:%s must be included", segment, version)
					} else {
						assert.Error(t, err, "%s:%s must be absent", segment, version)
					}
				}
			}
		})
	}
}
