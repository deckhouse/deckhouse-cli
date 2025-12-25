/*
Copyright 2024 Flant JSC

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

package mirror

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// DigestMap maps image reference (repo:tag) to its digest
type DigestMap map[string]string

// DigestCollector collects digests from a container registry
type DigestCollector struct {
	registry string
	auth     authn.Authenticator

	nameOpts   []name.Option
	remoteOpts []remote.Option
}

// NewDigestCollector creates a new digest collector
// tlsSkipVerify: skip TLS certificate verification (for self-signed certs)
func NewDigestCollector(registry string, authenticator authn.Authenticator, tlsSkipVerify bool) *DigestCollector {
	nameOpts := []name.Option{}
	remoteOpts := []remote.Option{}

	if tlsSkipVerify {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		remoteOpts = append(remoteOpts, remote.WithTransport(transport))
	}

	if authenticator != nil && authenticator != authn.Anonymous {
		remoteOpts = append(remoteOpts, remote.WithAuth(authenticator))
	}

	return &DigestCollector{
		registry:   registry,
		auth:       authenticator,
		nameOpts:   nameOpts,
		remoteOpts: remoteOpts,
	}
}

// CollectAll collects digests for all images in the registry
func (c *DigestCollector) CollectAll(ctx context.Context) (DigestMap, error) {
	digests := make(DigestMap)

	// Collect Deckhouse root images
	if err := c.collectDeckhouseRoot(ctx, digests); err != nil {
		return nil, fmt.Errorf("collect deckhouse root: %w", err)
	}

	// Collect install images
	if err := c.collectSegment(ctx, digests, internal.InstallSegment); err != nil {
		return nil, fmt.Errorf("collect install: %w", err)
	}

	// Collect install-standalone images
	if err := c.collectSegment(ctx, digests, internal.InstallStandaloneSegment); err != nil {
		return nil, fmt.Errorf("collect install-standalone: %w", err)
	}

	// Collect release-channel images
	if err := c.collectSegment(ctx, digests, internal.ReleaseChannelSegment); err != nil {
		return nil, fmt.Errorf("collect release-channel: %w", err)
	}

	// Collect security databases
	if err := c.collectSecurity(ctx, digests); err != nil {
		return nil, fmt.Errorf("collect security: %w", err)
	}

	// Collect modules
	if err := c.collectModules(ctx, digests); err != nil {
		return nil, fmt.Errorf("collect modules: %w", err)
	}

	return digests, nil
}

// collectDeckhouseRoot collects digests from the root registry path
func (c *DigestCollector) collectDeckhouseRoot(ctx context.Context, digests DigestMap) error {
	return c.collectTagsFromRepo(ctx, digests, c.registry)
}

// collectSegment collects digests from a specific segment (install, release-channel, etc)
func (c *DigestCollector) collectSegment(ctx context.Context, digests DigestMap, segment string) error {
	repo := path.Join(c.registry, segment)
	return c.collectTagsFromRepo(ctx, digests, repo)
}

// collectSecurity collects security database digests
func (c *DigestCollector) collectSecurity(ctx context.Context, digests DigestMap) error {
	securityImages := map[string]string{
		path.Join(c.registry, internal.SecuritySegment, internal.SecurityTrivyDBSegment):     "2",
		path.Join(c.registry, internal.SecuritySegment, internal.SecurityTrivyBDUSegment):    "1",
		path.Join(c.registry, internal.SecuritySegment, internal.SecurityTrivyJavaDBSegment): "1",
		path.Join(c.registry, internal.SecuritySegment, internal.SecurityTrivyChecksSegment): "0",
	}

	for repo, tag := range securityImages {
		digest, err := c.getDigest(ctx, repo+":"+tag)
		if err != nil {
			// Security databases might not exist, skip with warning
			continue
		}
		digests[repo+":"+tag] = digest
	}

	return nil
}

// collectModules collects digests for all modules
func (c *DigestCollector) collectModules(ctx context.Context, digests DigestMap) error {
	modulesRepo := path.Join(c.registry, internal.ModulesSegment)

	// List all modules (they are tags in the modules repo)
	modules, err := c.listTags(ctx, modulesRepo)
	if err != nil {
		// Modules might not be accessible
		return nil
	}

	for _, moduleName := range modules {
		// Module root images
		moduleRepo := path.Join(modulesRepo, moduleName)
		if err := c.collectTagsFromRepo(ctx, digests, moduleRepo); err != nil {
			continue
		}

		// Module release channels
		releaseRepo := path.Join(moduleRepo, "release")
		if err := c.collectTagsFromRepo(ctx, digests, releaseRepo); err != nil {
			// Release repo might not exist
			continue
		}
	}

	return nil
}

// collectTagsFromRepo lists all tags in a repo and collects their digests
func (c *DigestCollector) collectTagsFromRepo(ctx context.Context, digests DigestMap, repo string) error {
	tags, err := c.listTags(ctx, repo)
	if err != nil {
		return err
	}

	for _, tag := range tags {
		ref := repo + ":" + tag
		digest, err := c.getDigest(ctx, ref)
		if err != nil {
			continue
		}
		digests[ref] = digest
	}

	return nil
}

// listTags lists all tags in a repository
func (c *DigestCollector) listTags(ctx context.Context, repo string) ([]string, error) {
	repoRef, err := name.NewRepository(repo, c.nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("parse repo %s: %w", repo, err)
	}

	tags, err := remote.List(repoRef, c.remoteOpts...)
	if err != nil {
		return nil, fmt.Errorf("list tags for %s: %w", repo, err)
	}

	return tags, nil
}

// getDigest gets the digest for a specific image reference
func (c *DigestCollector) getDigest(ctx context.Context, ref string) (string, error) {
	imgRef, err := name.ParseReference(ref, c.nameOpts...)
	if err != nil {
		return "", fmt.Errorf("parse ref %s: %w", ref, err)
	}

	desc, err := remote.Head(imgRef, c.remoteOpts...)
	if err != nil {
		return "", fmt.Errorf("get digest for %s: %w", ref, err)
	}

	return desc.Digest.String(), nil
}

// NormalizeRef normalizes a reference to allow comparison between registries
// Strips the registry host from the reference
func NormalizeRef(ref string) string {
	// Remove scheme if present
	ref = strings.TrimPrefix(ref, "https://")
	ref = strings.TrimPrefix(ref, "http://")

	// Find the first slash after the host
	slashIdx := strings.Index(ref, "/")
	if slashIdx == -1 {
		return ref
	}

	// Return everything after the host
	return ref[slashIdx+1:]
}

// NormalizeDigests creates a new map with normalized references
func NormalizeDigests(digests DigestMap) DigestMap {
	normalized := make(DigestMap, len(digests))
	for ref, digest := range digests {
		normalized[NormalizeRef(ref)] = digest
	}
	return normalized
}
