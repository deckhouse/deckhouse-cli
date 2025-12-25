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
	"github.com/samber/lo"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// ValidationResult contains the results of structure validation
type ValidationResult struct {
	// MissingRepos lists repositories that should exist but don't
	MissingRepos []string

	// MissingTags maps repository to list of missing tags
	MissingTags map[string][]string

	// ExtraRepos lists unexpected repositories found
	ExtraRepos []string

	// Errors contains any errors encountered during validation
	Errors []error
}

// IsValid returns true if validation passed
func (r *ValidationResult) IsValid() bool {
	return len(r.MissingRepos) == 0 &&
		len(r.MissingTags) == 0 &&
		len(r.Errors) == 0
}

// String returns a human-readable summary
func (r *ValidationResult) String() string {
	var sb strings.Builder

	if len(r.MissingRepos) > 0 {
		sb.WriteString("Missing repositories:\n")
		for _, repo := range r.MissingRepos {
			sb.WriteString("  - " + repo + "\n")
		}
	}

	if len(r.MissingTags) > 0 {
		sb.WriteString("Missing tags:\n")
		for repo, tags := range r.MissingTags {
			sb.WriteString("  " + repo + ":\n")
			for _, tag := range tags {
				sb.WriteString("    - " + tag + "\n")
			}
		}
	}

	if len(r.Errors) > 0 {
		sb.WriteString("Errors:\n")
		for _, err := range r.Errors {
			sb.WriteString("  - " + err.Error() + "\n")
		}
	}

	if r.IsValid() {
		sb.WriteString("Validation passed\n")
	}

	return sb.String()
}

// StructureValidator validates the structure of a mirrored registry
type StructureValidator struct {
	registry string
	auth     authn.Authenticator

	nameOpts   []name.Option
	remoteOpts []remote.Option

	// Expected structure from reference registry
	expectedRepos []string
	expectedTags  map[string][]string
}

// NewStructureValidator creates a new structure validator
// tlsSkipVerify: skip TLS certificate verification (for self-signed certs)
func NewStructureValidator(registry string, authenticator authn.Authenticator, tlsSkipVerify bool) *StructureValidator {
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

	return &StructureValidator{
		registry:      registry,
		auth:          authenticator,
		nameOpts:      nameOpts,
		remoteOpts:    remoteOpts,
		expectedRepos: make([]string, 0),
		expectedTags:  make(map[string][]string),
	}
}

// SetExpectedFromDigests sets expected structure from a DigestMap
func (v *StructureValidator) SetExpectedFromDigests(digests DigestMap) {
	repoSet := make(map[string]struct{})
	tagsByRepo := make(map[string][]string)

	for ref := range digests {
		// Parse ref to extract repo and tag
		parts := strings.Split(ref, ":")
		if len(parts) != 2 {
			continue
		}
		repo := parts[0]
		tag := parts[1]

		repoSet[repo] = struct{}{}
		tagsByRepo[repo] = append(tagsByRepo[repo], tag)
	}

	v.expectedRepos = lo.Keys(repoSet)
	v.expectedTags = tagsByRepo
}

// Validate checks the registry structure
func (v *StructureValidator) Validate(ctx context.Context) (*ValidationResult, error) {
	result := &ValidationResult{
		MissingRepos: make([]string, 0),
		MissingTags:  make(map[string][]string),
		ExtraRepos:   make([]string, 0),
		Errors:       make([]error, 0),
	}

	// Validate each expected repository
	for _, expectedRepo := range v.expectedRepos {
		actualRepo := v.translateRepo(expectedRepo)

		// Check if repo exists by trying to list tags
		tags, err := v.listTags(ctx, actualRepo)
		if err != nil {
			result.MissingRepos = append(result.MissingRepos, actualRepo)
			result.Errors = append(result.Errors, fmt.Errorf("repo %s: %w", actualRepo, err))
			continue
		}

		// Check for missing tags
		expectedTags := v.expectedTags[expectedRepo]
		missingTags := lo.Filter(expectedTags, func(tag string, _ int) bool {
			return !lo.Contains(tags, tag)
		})

		if len(missingTags) > 0 {
			result.MissingTags[actualRepo] = missingTags
		}
	}

	return result, nil
}

// ValidateMinimal performs minimal validation (just checks key segments exist)
func (v *StructureValidator) ValidateMinimal(ctx context.Context) (*ValidationResult, error) {
	result := &ValidationResult{
		MissingRepos: make([]string, 0),
		MissingTags:  make(map[string][]string),
		Errors:       make([]error, 0),
	}

	// Check required segments
	segments := []string{
		"",                                // root
		internal.InstallSegment,           // install
		internal.InstallStandaloneSegment, // install-standalone
		internal.ReleaseChannelSegment,    // release-channel
	}

	for _, segment := range segments {
		repo := v.registry
		if segment != "" {
			repo = path.Join(v.registry, segment)
		}

		tags, err := v.listTags(ctx, repo)
		if err != nil {
			result.MissingRepos = append(result.MissingRepos, repo)
			result.Errors = append(result.Errors, fmt.Errorf("segment %s: %w", segment, err))
			continue
		}

		// Check that at least one release channel tag exists
		hasReleaseChannel := lo.ContainsBy(tags, func(tag string) bool {
			return lo.Contains(internal.GetAllDefaultReleaseChannels(), tag)
		})
		if !hasReleaseChannel {
			result.MissingTags[repo] = []string{"<at least one release channel>"}
		}
	}

	return result, nil
}

// translateRepo translates a source repo path to target repo path
func (v *StructureValidator) translateRepo(sourceRepo string) string {
	// Normalize the source repo to get the path portion
	normalizedPath := NormalizeRef(sourceRepo)
	// Prepend the target registry
	return path.Join(v.registry, normalizedPath)
}

// listTags lists all tags in a repository
func (v *StructureValidator) listTags(ctx context.Context, repo string) ([]string, error) {
	repoRef, err := name.NewRepository(repo, v.nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("parse repo: %w", err)
	}

	tags, err := remote.List(repoRef, v.remoteOpts...)
	if err != nil {
		return nil, fmt.Errorf("list tags: %w", err)
	}

	return tags, nil
}
