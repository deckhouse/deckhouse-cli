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

package internal

import (
	"archive/tar"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	d8internal "github.com/deckhouse/deckhouse-cli/internal"
)

func InsecureTransport() http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	return transport
}

type SourceReader struct {
	registry   string
	auth       authn.Authenticator
	opts       []remote.Option
	progressFn func(string)
}

func NewSourceReader(registry string, auth authn.Authenticator, tlsSkipVerify bool) *SourceReader {
	opts := []remote.Option{remote.WithAuth(auth)}
	if tlsSkipVerify {
		opts = append(opts, remote.WithTransport(InsecureTransport()))
	}

	return &SourceReader{
		registry: registry,
		auth:     auth,
		opts:     opts,
	}
}

func (r *SourceReader) SetProgressCallback(fn func(string)) {
	r.progressFn = fn
}

func (r *SourceReader) Registry() string {
	return r.registry
}

func (r *SourceReader) RemoteOpts() []remote.Option {
	return r.opts
}

func (r *SourceReader) progress(format string, args ...interface{}) {
	if r.progressFn != nil {
		r.progressFn(fmt.Sprintf(format, args...))
	}
}

type ReleaseChannelInfo struct {
	Channel string
	Version string
}

func (r *SourceReader) ReadReleaseChannels(ctx context.Context) ([]ReleaseChannelInfo, error) {
	channels := d8internal.GetAllDefaultReleaseChannels()
	result := make([]ReleaseChannelInfo, 0, len(channels))

	for _, channel := range channels {
		r.progress("Reading release channel: %s", channel)

		ref := path.Join(r.registry, d8internal.ReleaseChannelSegment) + ":" + channel
		version, err := r.readReleaseChannelVersion(ctx, ref)
		if err != nil {
			r.progress("  Warning: failed to read %s: %v", channel, err)
			continue
		}

		result = append(result, ReleaseChannelInfo{
			Channel: channel,
			Version: version,
		})
		r.progress("  %s -> %s", channel, version)
	}

	return result, nil
}

func (r *SourceReader) readReleaseChannelVersion(ctx context.Context, ref string) (string, error) {
	imgRef, err := name.ParseReference(ref)
	if err != nil {
		return "", fmt.Errorf("parse reference: %w", err)
	}

	img, err := remote.Image(imgRef, r.opts...)
	if err != nil {
		return "", fmt.Errorf("get image: %w", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return "", fmt.Errorf("get layers: %w", err)
	}

	for _, layer := range layers {
		rc, err := layer.Uncompressed()
		if err != nil {
			continue
		}

		version, err := extractVersionFromTar(rc)
		rc.Close()
		if err == nil && version != "" {
			return version, nil
		}
	}

	return "", fmt.Errorf("version.json not found in image")
}

func extractVersionFromTar(rc io.Reader) (string, error) {
	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		if strings.HasSuffix(hdr.Name, "version.json") {
			var meta struct {
				Version string `json:"version"`
			}
			if err := json.NewDecoder(tr).Decode(&meta); err != nil {
				return "", err
			}
			return meta.Version, nil
		}
	}
	return "", nil
}

type PlatformDigests struct {
	Versions       []string
	InstallDigests map[string]string
	ImageDigests   []string
}

func (r *SourceReader) ReadPlatformDigests(ctx context.Context, channels []ReleaseChannelInfo) (*PlatformDigests, error) {
	result := &PlatformDigests{
		InstallDigests: make(map[string]string),
		ImageDigests:   make([]string, 0),
	}

	versionSet := make(map[string]bool)
	for _, ch := range channels {
		versionSet[ch.Version] = true
	}

	for version := range versionSet {
		result.Versions = append(result.Versions, version)
	}
	sort.Strings(result.Versions)

	digestSet := make(map[string]bool)

	for _, version := range result.Versions {
		r.progress("Reading install:%s digests...", version)

		tag := version
		if !strings.HasPrefix(tag, "v") {
			if _, err := semver.NewVersion(version); err == nil {
				tag = "v" + tag
			}
		}

		installRef := path.Join(r.registry, d8internal.InstallSegment) + ":" + tag
		digests, err := r.readInstallDigests(ctx, installRef)
		if err != nil {
			r.progress("  Warning: failed to read install:%s: %v", tag, err)
			continue
		}

		r.progress("  Found %d digests", len(digests))
		for _, d := range digests {
			digestSet[d] = true
		}
	}

	for d := range digestSet {
		result.ImageDigests = append(result.ImageDigests, d)
	}
	sort.Strings(result.ImageDigests)

	return result, nil
}

func (r *SourceReader) readInstallDigests(ctx context.Context, ref string) ([]string, error) {
	imgRef, err := name.ParseReference(ref)
	if err != nil {
		return nil, fmt.Errorf("parse reference: %w", err)
	}

	desc, err := remote.Get(imgRef, r.opts...)
	if err != nil {
		return nil, fmt.Errorf("get descriptor: %w", err)
	}

	var img v1.Image

	if desc.MediaType.IsIndex() {
		idx, err := desc.ImageIndex()
		if err != nil {
			return nil, fmt.Errorf("get index: %w", err)
		}

		manifest, err := idx.IndexManifest()
		if err != nil {
			return nil, fmt.Errorf("get index manifest: %w", err)
		}

		if len(manifest.Manifests) == 0 {
			return nil, fmt.Errorf("index has no manifests")
		}

		img, err = idx.Image(manifest.Manifests[0].Digest)
		if err != nil {
			return nil, fmt.Errorf("get image from index: %w", err)
		}
	} else {
		img, err = desc.Image()
		if err != nil {
			return nil, fmt.Errorf("get image: %w", err)
		}
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("get layers: %w", err)
	}

	for _, layer := range layers {
		rc, err := layer.Uncompressed()
		if err != nil {
			continue
		}

		digests, err := extractDigestsFromTar(rc)
		rc.Close()
		if err == nil && len(digests) > 0 {
			return digests, nil
		}
	}

	return nil, fmt.Errorf("images_digests.json not found")
}

func extractDigestsFromTar(rc io.Reader) ([]string, error) {
	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if strings.HasSuffix(hdr.Name, "deckhouse/candi/images_digests.json") ||
			hdr.Name == "deckhouse/candi/images_digests.json" {
			var digestsByModule map[string]map[string]string
			if err := json.NewDecoder(tr).Decode(&digestsByModule); err != nil {
				return nil, err
			}

			var result []string
			for _, images := range digestsByModule {
				for _, digest := range images {
					result = append(result, digest)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

type ModuleInfo struct {
	Name            string
	ReleaseChannels []ReleaseChannelInfo
	Versions        []string
	ImageDigests    []string
}

func (r *SourceReader) listTags(ctx context.Context, repoPath string) ([]string, error) {
	ref, err := name.ParseReference(repoPath + ":latest")
	if err != nil {
		return nil, fmt.Errorf("parse reference: %w", err)
	}

	repo := ref.Context()
	opts := append(r.opts, remote.WithContext(ctx))
	tags, err := remote.List(repo, opts...)
	if err != nil {
		return nil, fmt.Errorf("list tags: %w", err)
	}

	return tags, nil
}

func (r *SourceReader) ReadModulesList(ctx context.Context) ([]string, error) {
	r.progress("Discovering modules...")

	modulesRef := path.Join(r.registry, d8internal.ModulesSegment)
	tags, err := r.listTags(ctx, modulesRef)
	if err != nil {
		return nil, fmt.Errorf("list modules: %w", err)
	}

	r.progress("Found %d modules", len(tags))
	return tags, nil
}

func (r *SourceReader) ReadModuleDigests(ctx context.Context, moduleName string) (*ModuleInfo, error) {
	r.progress("Reading module %s...", moduleName)

	info := &ModuleInfo{
		Name:            moduleName,
		ReleaseChannels: make([]ReleaseChannelInfo, 0),
		Versions:        make([]string, 0),
		ImageDigests:    make([]string, 0),
	}

	channels := d8internal.GetAllDefaultReleaseChannels()
	moduleReleaseBase := path.Join(r.registry, d8internal.ModulesSegment, moduleName, "release")

	versionSet := make(map[string]bool)
	for _, channel := range channels {
		ref := moduleReleaseBase + ":" + channel
		version, err := r.readModuleReleaseChannelVersion(ctx, ref)
		if err != nil {
			continue
		}

		info.ReleaseChannels = append(info.ReleaseChannels, ReleaseChannelInfo{
			Channel: channel,
			Version: version,
		})
		versionSet[version] = true
	}

	if len(info.ReleaseChannels) == 0 {
		r.progress("  No release channels found")
		return info, nil
	}

	r.progress("  Found %d release channels", len(info.ReleaseChannels))

	for version := range versionSet {
		info.Versions = append(info.Versions, version)
	}
	sort.Strings(info.Versions)

	digestSet := make(map[string]bool)
	moduleBase := path.Join(r.registry, d8internal.ModulesSegment, moduleName)

	for _, version := range info.Versions {
		moduleRef := moduleBase + ":" + version
		digests, err := r.readModuleImageDigests(ctx, moduleRef)
		if err != nil {
			r.progress("  Warning: failed to read digests for %s:%s: %v", moduleName, version, err)
			continue
		}

		for _, d := range digests {
			digestSet[d] = true
		}
	}

	for d := range digestSet {
		info.ImageDigests = append(info.ImageDigests, d)
	}
	sort.Strings(info.ImageDigests)

	r.progress("  Found %d versions, %d unique digests", len(info.Versions), len(info.ImageDigests))

	return info, nil
}

func (r *SourceReader) readModuleReleaseChannelVersion(ctx context.Context, ref string) (string, error) {
	imgRef, err := name.ParseReference(ref)
	if err != nil {
		return "", fmt.Errorf("parse reference: %w", err)
	}

	opts := append(r.opts, remote.WithContext(ctx))
	img, err := remote.Image(imgRef, opts...)
	if err != nil {
		return "", fmt.Errorf("get image: %w", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return "", fmt.Errorf("get layers: %w", err)
	}

	for _, layer := range layers {
		rc, err := layer.Uncompressed()
		if err != nil {
			continue
		}

		version, err := extractVersionFromTar(rc)
		rc.Close()
		if err == nil && version != "" {
			return version, nil
		}
	}

	return "", fmt.Errorf("version.json not found in image")
}

func (r *SourceReader) readModuleImageDigests(ctx context.Context, ref string) ([]string, error) {
	imgRef, err := name.ParseReference(ref)
	if err != nil {
		return nil, fmt.Errorf("parse reference: %w", err)
	}

	opts := append(r.opts, remote.WithContext(ctx))
	desc, err := remote.Get(imgRef, opts...)
	if err != nil {
		return nil, fmt.Errorf("get descriptor: %w", err)
	}

	var img v1.Image

	if desc.MediaType.IsIndex() {
		idx, err := desc.ImageIndex()
		if err != nil {
			return nil, fmt.Errorf("get index: %w", err)
		}

		manifest, err := idx.IndexManifest()
		if err != nil {
			return nil, fmt.Errorf("get index manifest: %w", err)
		}

		if len(manifest.Manifests) == 0 {
			return nil, fmt.Errorf("index has no manifests")
		}

		img, err = idx.Image(manifest.Manifests[0].Digest)
		if err != nil {
			return nil, fmt.Errorf("get image from index: %w", err)
		}
	} else {
		img, err = desc.Image()
		if err != nil {
			return nil, fmt.Errorf("get image: %w", err)
		}
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("get layers: %w", err)
	}

	for _, layer := range layers {
		rc, err := layer.Uncompressed()
		if err != nil {
			continue
		}

		digests, err := extractModuleDigestsFromTar(rc)
		rc.Close()
		if err == nil && len(digests) > 0 {
			return digests, nil
		}
	}

	return nil, nil
}

func extractModuleDigestsFromTar(rc io.Reader) ([]string, error) {
	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if hdr.Name == "images_digests.json" || strings.HasSuffix(hdr.Name, "/images_digests.json") {
			var digestsByComponent map[string]map[string]string
			if err := json.NewDecoder(tr).Decode(&digestsByComponent); err != nil {
				return nil, err
			}

			var result []string
			for _, images := range digestsByComponent {
				for _, digest := range images {
					result = append(result, digest)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

type SecurityDigests struct {
	Databases map[string][]string
}

func (r *SourceReader) ReadSecurityDigests(ctx context.Context) (*SecurityDigests, error) {
	r.progress("Reading security databases...")

	result := &SecurityDigests{
		Databases: make(map[string][]string),
	}

	databases := []string{
		d8internal.SecurityTrivyDBSegment,
		d8internal.SecurityTrivyBDUSegment,
		d8internal.SecurityTrivyJavaDBSegment,
		d8internal.SecurityTrivyChecksSegment,
	}

	for _, db := range databases {
		dbRef := path.Join(r.registry, d8internal.SecuritySegment, db)
		tags, err := r.listTags(ctx, dbRef)
		if err != nil {
			r.progress("  %s: not found", db)
			continue
		}

		result.Databases[db] = tags
		r.progress("  %s: %d tags", db, len(tags))
	}

	return result, nil
}

type ExpectedImages struct {
	Platform *PlatformDigests
	Modules  []*ModuleInfo
	Security *SecurityDigests
}

func (r *SourceReader) ReadAllExpected(ctx context.Context) (*ExpectedImages, error) {
	result := &ExpectedImages{}

	channels, err := r.ReadReleaseChannels(ctx)
	if err != nil {
		return nil, fmt.Errorf("read release channels: %w", err)
	}

	result.Platform, err = r.ReadPlatformDigests(ctx, channels)
	if err != nil {
		return nil, fmt.Errorf("read platform digests: %w", err)
	}

	modules, err := r.ReadModulesList(ctx)
	if err != nil {
		r.progress("Warning: failed to read modules: %v", err)
	} else {
		for _, moduleName := range modules {
			info, err := r.ReadModuleDigests(ctx, moduleName)
			if err != nil {
				r.progress("Warning: failed to read module %s: %v", moduleName, err)
				continue
			}
			result.Modules = append(result.Modules, info)
		}
	}

	result.Security, err = r.ReadSecurityDigests(ctx)
	if err != nil {
		r.progress("Warning: failed to read security: %v", err)
	}

	return result, nil
}

