/*
Copyright 2025 Flant JSC

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

package platform

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/samber/lo"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
)

// Compile-time interface check
var _ usecase.PlatformPuller = (*PlatformService)(nil)

// PlatformService handles pulling Deckhouse platform images using Clean Architecture
type PlatformService struct {
	// Dependencies (injected via interfaces)
	registry     usecase.DeckhouseImageService
	rootURL      string
	bundlePacker usecase.BundlePacker
	logger       usecase.Logger

	// Internal state
	layout       *Layouts
	downloadList *DownloadList

	// Configuration
	opts *usecase.PlatformOpts
}

// NewPlatformService creates a new platform service with injected dependencies
func NewPlatformService(
	registry usecase.DeckhouseRegistryService,
	bundlePacker usecase.BundlePacker,
	logger usecase.Logger,
	opts *usecase.PlatformOpts,
) *PlatformService {
	if opts == nil {
		opts = &usecase.PlatformOpts{}
	}

	rootURL := registry.GetRoot()

	return &PlatformService{
		registry:     registry.Deckhouse(),
		rootURL:      rootURL,
		bundlePacker: bundlePacker,
		logger:       logger,
		downloadList: NewDownloadList(rootURL),
		opts:         opts,
	}
}

// Pull implements usecase.PlatformPuller
func (s *PlatformService) Pull(ctx context.Context) error {
	// Initialize layouts
	if err := s.initLayouts(); err != nil {
		return fmt.Errorf("init layouts: %w", err)
	}

	// Validate access to registry
	if err := s.validateAccess(ctx); err != nil {
		return fmt.Errorf("validate access: %w", err)
	}

	// Find tags to mirror
	tags, err := s.findTags(ctx)
	if err != nil {
		return fmt.Errorf("find tags to mirror: %w", err)
	}

	s.logger.Infof("Tags to mirror: %v", tags)

	// Fill download list
	s.downloadList.FillDeckhouseImages(tags)
	s.downloadList.FillForTag(s.opts.TargetTag)

	// Pull images
	if err := s.pullAllImages(ctx); err != nil {
		return fmt.Errorf("pull images: %w", err)
	}

	// Pack bundle
	if err := s.bundlePacker.Pack(ctx, s.layout.WorkingDir(), "platform.tar"); err != nil {
		return fmt.Errorf("pack bundle: %w", err)
	}

	return nil
}

func (s *PlatformService) initLayouts() error {
	s.logger.Info("Creating OCI Image Layouts for platform")

	layouts, err := NewLayouts(s.opts.BundleDir)
	if err != nil {
		return fmt.Errorf("create layouts: %w", err)
	}

	s.layout = layouts
	return nil
}

func (s *PlatformService) validateAccess(ctx context.Context) error {
	targetTag := internal.StableChannel
	if s.opts.TargetTag != "" {
		targetTag = s.opts.TargetTag
	}

	s.logger.Debugf("Validating access to registry with tag: %s", targetTag)

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Check if target is a release channel or specific tag
	if internal.ChannelIsValid(targetTag) {
		if err := s.registry.ReleaseChannels().CheckImageExists(ctx, targetTag); err != nil {
			return fmt.Errorf("release channel %s not accessible: %w", targetTag, err)
		}
		return nil
	}

	// Check specific tag
	if err := s.registry.CheckImageExists(ctx, targetTag); err != nil {
		return fmt.Errorf("tag %s not accessible: %w", targetTag, err)
	}

	return nil
}

func (s *PlatformService) findTags(ctx context.Context) ([]string, error) {
	if s.opts.TargetTag != "" {
		s.logger.Infof("Using specific tag: %s", s.opts.TargetTag)
		return []string{s.opts.TargetTag}, nil
	}

	versions, err := s.findVersions(ctx)
	if err != nil {
		return nil, err
	}

	return lo.Map(versions, func(v semver.Version, _ int) string {
		return "v" + v.String()
	}), nil
}

func (s *PlatformService) findVersions(ctx context.Context) ([]semver.Version, error) {
	releaseChannels := append(internal.GetAllDefaultReleaseChannels(), internal.LTSChannel)
	channelVersions := make(map[string]*semver.Version, len(releaseChannels))

	// Get versions from release channels
	for _, channel := range releaseChannels {
		version, err := s.getChannelVersion(ctx, channel)
		if err != nil {
			if channel == internal.LTSChannel {
				if !errors.Is(err, usecase.ErrImageNotFound) {
					s.logger.Warnf("Skipping LTS channel: %v", err)
				}
				continue
			}
			return nil, fmt.Errorf("get %s channel version: %w", channel, err)
		}
		if version != nil {
			channelVersions[channel] = version
		}
	}

	rockSolidVersion := channelVersions[internal.RockSolidChannel]
	if rockSolidVersion == nil {
		return nil, fmt.Errorf("rock-solid channel version not found")
	}

	minVersion := *rockSolidVersion
	if s.opts.SinceVersion != nil && s.opts.SinceVersion.LessThan(rockSolidVersion) {
		minVersion = *s.opts.SinceVersion
	}

	// List all available tags
	tags, err := s.registry.ReleaseChannels().ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tags: %w", err)
	}

	alphaVersion := channelVersions[internal.AlphaChannel]
	if alphaVersion == nil {
		return nil, fmt.Errorf("alpha channel version not found")
	}

	// Filter versions
	filteredVersions := filterVersions(&minVersion, alphaVersion, tags)
	filteredVersions = latestPatches(filteredVersions)

	// Collect all channel versions
	allVersions := make([]*semver.Version, 0, len(channelVersions)+len(filteredVersions))
	for _, v := range channelVersions {
		allVersions = append(allVersions, v)
	}
	allVersions = append(allVersions, filteredVersions...)

	return dedupVersions(allVersions), nil
}

func (s *PlatformService) getChannelVersion(ctx context.Context, channel string) (*semver.Version, error) {
	meta, err := s.registry.ReleaseChannels().GetMetadata(ctx, channel)
	if err != nil {
		return nil, err
	}

	version, err := semver.NewVersion(meta.Version)
	if err != nil {
		return nil, fmt.Errorf("invalid version %q: %w", meta.Version, err)
	}

	// Store image in layout for later use
	img, err := s.registry.ReleaseChannels().GetImage(ctx, channel)
	if err != nil {
		return nil, fmt.Errorf("get channel image: %w", err)
	}

	if err := s.layout.ReleaseChannels().AddImage(img, channel); err != nil {
		return nil, fmt.Errorf("add channel image to layout: %w", err)
	}

	return version, nil
}

func (s *PlatformService) pullAllImages(ctx context.Context) error {
	// Pull release channels
	if err := s.pullReleaseChannelImages(ctx); err != nil {
		return fmt.Errorf("pull release channels: %w", err)
	}

	// Pull installers
	if err := s.pullInstallerImages(ctx); err != nil {
		return fmt.Errorf("pull installers: %w", err)
	}

	// Pull standalone installers
	if err := s.pullStandaloneInstallerImages(ctx); err != nil {
		return fmt.Errorf("pull standalone installers: %w", err)
	}

	// Pull main Deckhouse images
	if err := s.pullMainImages(ctx); err != nil {
		return fmt.Errorf("pull deckhouse images: %w", err)
	}

	return nil
}

func (s *PlatformService) pullReleaseChannelImages(ctx context.Context) error {
	return s.logger.Process("Pull release channels", func() error {
		for ref := range s.downloadList.ReleaseChannels {
			_, tag := splitRef(ref)

			img, err := s.registry.ReleaseChannels().GetImage(ctx, tag)
			if err != nil {
				if s.opts.TargetTag != "" {
					s.logger.Warnf("Release channel %s not found, skipping", tag)
					continue
				}
				return fmt.Errorf("get release channel %s: %w", tag, err)
			}

			if err := s.layout.ReleaseChannels().AddImage(img, tag); err != nil {
				return fmt.Errorf("add release channel to layout: %w", err)
			}
		}
		return nil
	})
}

func (s *PlatformService) pullInstallerImages(ctx context.Context) error {
	return s.logger.Process("Pull installers", func() error {
		for ref := range s.downloadList.Installers {
			_, tag := splitRef(ref)

			img, err := s.registry.Installer().GetImage(ctx, tag)
			if err != nil {
				s.logger.Warnf("Installer %s not found, skipping", tag)
				continue
			}

			if err := s.layout.Installer().AddImage(img, tag); err != nil {
				return fmt.Errorf("add installer to layout: %w", err)
			}
		}
		return nil
	})
}

func (s *PlatformService) pullStandaloneInstallerImages(ctx context.Context) error {
	return s.logger.Process("Pull standalone installers", func() error {
		for ref := range s.downloadList.StandaloneInstallers {
			_, tag := splitRef(ref)

			img, err := s.registry.StandaloneInstaller().GetImage(ctx, tag)
			if err != nil {
				s.logger.Warnf("Standalone installer %s not found, skipping", tag)
				continue
			}

			if err := s.layout.StandaloneInstaller().AddImage(img, tag); err != nil {
				return fmt.Errorf("add standalone installer to layout: %w", err)
			}
		}
		return nil
	})
}

func (s *PlatformService) pullMainImages(ctx context.Context) error {
	return s.logger.Process("Pull Deckhouse images", func() error {
		total := len(s.downloadList.Images)
		current := 0

		for ref := range s.downloadList.Images {
			current++
			_, tag := splitRef(ref)

			s.logger.Infof("[%d/%d] Pulling %s", current, total, ref)

			img, err := s.registry.GetImage(ctx, tag)
			if err != nil {
				return fmt.Errorf("get image %s: %w", ref, err)
			}

			if err := s.layout.Deckhouse().AddImage(img, tag); err != nil {
				return fmt.Errorf("add image to layout: %w", err)
			}
		}
		return nil
	})
}

// Helper functions with unique names to avoid conflicts with platform.go

func splitRef(ref string) (repo, tag string) {
	for i := len(ref) - 1; i >= 0; i-- {
		if ref[i] == ':' {
			return ref[:i], ref[i+1:]
		}
		if ref[i] == '@' {
			return ref[:i], ref[i:]
		}
	}
	return ref, ""
}

func filterVersions(min, max *semver.Version, tags []string) []*semver.Version {
	result := make([]*semver.Version, 0)
	for _, tag := range tags {
		v, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}
		if min.GreaterThan(v) || v.GreaterThan(max) {
			continue
		}
		result = append(result, v)
	}
	return result
}

func latestPatches(versions []*semver.Version) []*semver.Version {
	type majorMinor [2]uint64
	patches := map[majorMinor]uint64{}

	for _, v := range versions {
		key := majorMinor{v.Major(), v.Minor()}
		if patch := patches[key]; patch <= v.Patch() {
			patches[key] = v.Patch()
		}
	}

	result := make([]*semver.Version, 0, len(patches))
	for mm, patch := range patches {
		result = append(result, semver.MustParse(fmt.Sprintf("v%d.%d.%d", mm[0], mm[1], patch)))
	}
	return result
}

func dedupVersions(versions []*semver.Version) []semver.Version {
	seen := make(map[string]struct{})
	result := make([]semver.Version, 0, len(versions))

	for _, v := range versions {
		key := v.String()
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, *v)
		}
	}
	return result
}
