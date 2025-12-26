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
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// Tags to exclude from comparison (not mirrored by design)
var (
	// Digest-based tags (sha256 hashes used as tags)
	digestTagRegex = regexp.MustCompile(`^[a-f0-9]{64}$`)
	// SHA256 prefixed tags
	sha256TagRegex = regexp.MustCompile(`^sha256-[a-f0-9]{64}`)
	// Cosign signature and attestation tags
	cosignTagSuffixes = []string{".sig", ".att", ".sbom"}
	// Service tags
	serviceTags = []string{"d8WriteCheck"}
)

// shouldSkipTag returns true if the tag should be excluded from comparison
func shouldSkipTag(tag string) bool {
	// Skip digest-based tags
	if digestTagRegex.MatchString(tag) {
		return true
	}
	// Skip sha256- prefixed tags
	if sha256TagRegex.MatchString(tag) {
		return true
	}
	// Skip cosign tags
	for _, suffix := range cosignTagSuffixes {
		if strings.HasSuffix(tag, suffix) {
			return true
		}
	}
	// Skip service tags
	for _, svcTag := range serviceTags {
		if tag == svcTag {
			return true
		}
	}
	return false
}

// ImageInfo contains detailed information about an image
type ImageInfo struct {
	Reference    string
	Digest       string   // manifest digest
	ConfigDigest string   // config digest
	Layers       []string // layer digests
	TotalSize    int64    // total size in bytes
}

// RegistryComparator performs deep comparison between source and target registries
type RegistryComparator struct {
	sourceRegistry string
	targetRegistry string
	sourceAuth     authn.Authenticator
	targetAuth     authn.Authenticator

	nameOpts         []name.Option
	sourceRemoteOpts []remote.Option
	targetRemoteOpts []remote.Option

	// Progress callback
	onProgress func(msg string)
}

// NewRegistryComparator creates a new registry comparator
func NewRegistryComparator(
	sourceRegistry, targetRegistry string,
	sourceAuth, targetAuth authn.Authenticator,
	tlsSkipVerify bool,
) *RegistryComparator {
	nameOpts := []name.Option{}
	sourceRemoteOpts := []remote.Option{}
	targetRemoteOpts := []remote.Option{}

	if tlsSkipVerify {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		sourceRemoteOpts = append(sourceRemoteOpts, remote.WithTransport(transport))
		targetRemoteOpts = append(targetRemoteOpts, remote.WithTransport(transport))
	}

	if sourceAuth != nil && sourceAuth != authn.Anonymous {
		sourceRemoteOpts = append(sourceRemoteOpts, remote.WithAuth(sourceAuth))
	}
	if targetAuth != nil && targetAuth != authn.Anonymous {
		targetRemoteOpts = append(targetRemoteOpts, remote.WithAuth(targetAuth))
	}

	return &RegistryComparator{
		sourceRegistry:   sourceRegistry,
		targetRegistry:   targetRegistry,
		sourceAuth:       sourceAuth,
		targetAuth:       targetAuth,
		nameOpts:         nameOpts,
		sourceRemoteOpts: sourceRemoteOpts,
		targetRemoteOpts: targetRemoteOpts,
	}
}

// SetProgressCallback sets a callback for progress updates
func (c *RegistryComparator) SetProgressCallback(fn func(msg string)) {
	c.onProgress = fn
}

func (c *RegistryComparator) logProgressf(format string, args ...interface{}) {
	if c.onProgress != nil {
		c.onProgress(fmt.Sprintf(format, args...))
	}
}

// ComparisonReport contains detailed comparison results
type ComparisonReport struct {
	StartTime time.Time
	EndTime   time.Time

	SourceRegistry string
	TargetRegistry string

	// Repository-level stats
	SourceRepositories  []string
	TargetRepositories  []string
	MissingRepositories []string // In source but not in target
	ExtraRepositories   []string // In target but not in source

	// Tag-level stats per repository
	RepositoryDetails map[string]*RepositoryComparison

	// Image-level stats
	TotalSourceImages int
	TotalTargetImages int
	SkippedImages     int // Digest-based, .att, .sig tags (not mirrored by design)
	MatchedImages     int
	MismatchedImages  []ImageMismatch
	MissingImages     []string // ref in source but not in target
	ExtraImages       []string // ref in target but not in source

	// Deep comparison stats
	DeepCheckedImages int
	TotalSourceLayers int
	TotalTargetLayers int
	MatchedLayers     int
	MissingLayers     int
	ConfigMismatches  int
	LayerMismatches   []LayerMismatch
}

// RepositoryComparison holds comparison for a single repository
type RepositoryComparison struct {
	Repository  string
	SourceTags  []string
	TargetTags  []string
	MissingTags []string // In source but not in target
	ExtraTags   []string // In target but not in source
	SkippedTags int      // Tags skipped (digest-based, .att, .sig, etc.)
	MatchedTags int
	TagDetails  map[string]*TagComparison
}

// TagComparison holds comparison for a single tag
type TagComparison struct {
	Tag           string
	SourceDigest  string
	TargetDigest  string
	Match         bool
	SourceConfig  string
	TargetConfig  string
	ConfigMatch   bool
	SourceLayers  []string
	TargetLayers  []string
	MissingLayers []string
	ExtraLayers   []string
	LayersMatch   bool
	DeepChecked   bool // true if layers were compared
}

// ImageMismatch represents a digest mismatch for an image
type ImageMismatch struct {
	Reference    string
	SourceDigest string
	TargetDigest string
}

// LayerMismatch represents a missing or different layer
type LayerMismatch struct {
	Reference   string
	LayerDigest string
	Reason      string // "missing", "size_mismatch"
}

// IsIdentical returns true if registries are identical
func (r *ComparisonReport) IsIdentical() bool {
	return len(r.MissingRepositories) == 0 &&
		len(r.MissingImages) == 0 &&
		len(r.MismatchedImages) == 0 &&
		len(r.LayerMismatches) == 0 &&
		r.MissingLayers == 0 &&
		r.ConfigMismatches == 0
}

// Summary returns a summary string
func (r *ComparisonReport) Summary() string {
	var sb strings.Builder

	sb.WriteString("REGISTRY COMPARISON SUMMARY\n")
	sb.WriteString("===========================\n\n")

	sb.WriteString(fmt.Sprintf("Source: %s\n", r.SourceRegistry))
	sb.WriteString(fmt.Sprintf("Target: %s\n", r.TargetRegistry))
	sb.WriteString(fmt.Sprintf("Duration: %s\n\n", r.EndTime.Sub(r.StartTime).Round(time.Second)))

	sb.WriteString("REPOSITORIES:\n")
	sb.WriteString(fmt.Sprintf("  Source: %d\n", len(r.SourceRepositories)))
	sb.WriteString(fmt.Sprintf("  Target: %d\n", len(r.TargetRepositories)))
	sb.WriteString(fmt.Sprintf("  Missing in target: %d\n", len(r.MissingRepositories)))
	sb.WriteString(fmt.Sprintf("  Extra in target: %d\n\n", len(r.ExtraRepositories)))

	sb.WriteString("IMAGES TO VERIFY:\n")
	sb.WriteString(fmt.Sprintf("  Source: %d images\n", r.TotalSourceImages))
	sb.WriteString(fmt.Sprintf("  Target: %d images\n", r.TotalTargetImages))
	if r.SkippedImages > 0 {
		sb.WriteString(fmt.Sprintf("  (excluded %d internal tags: digest-based, .att, .sig)\n", r.SkippedImages))
	}
	sb.WriteString("\n")
	sb.WriteString("VERIFICATION RESULTS:\n")
	sb.WriteString(fmt.Sprintf("  Matched: %d\n", r.MatchedImages))
	if len(r.MissingImages) > 0 {
		sb.WriteString(fmt.Sprintf("  Missing in target: %d\n", len(r.MissingImages)))
	}
	if len(r.MismatchedImages) > 0 {
		sb.WriteString(fmt.Sprintf("  Digest mismatch: %d\n", len(r.MismatchedImages)))
	}
	if len(r.ExtraImages) > 0 {
		sb.WriteString(fmt.Sprintf("  Extra in target: %d\n", len(r.ExtraImages)))
	}

	if r.DeepCheckedImages > 0 {
		sb.WriteString("DEEP COMPARISON (layers + config):\n")
		sb.WriteString(fmt.Sprintf("  Images deep-checked: %d\n", r.DeepCheckedImages))
		sb.WriteString(fmt.Sprintf("  Source layers: %d\n", r.TotalSourceLayers))
		sb.WriteString(fmt.Sprintf("  Target layers: %d\n", r.TotalTargetLayers))
		sb.WriteString(fmt.Sprintf("  Matched layers: %d\n", r.MatchedLayers))
		sb.WriteString(fmt.Sprintf("  Missing layers: %d\n", r.MissingLayers))
		sb.WriteString(fmt.Sprintf("  Config mismatches: %d\n\n", r.ConfigMismatches))
	}

	if r.IsIdentical() {
		sb.WriteString("✓ REGISTRIES ARE IDENTICAL (all hashes match)\n")
	} else {
		sb.WriteString("✗ REGISTRIES DIFFER\n")
	}

	return sb.String()
}

// DetailedReport returns a detailed report string
func (r *ComparisonReport) DetailedReport() string {
	var sb strings.Builder

	sb.WriteString(r.Summary())
	sb.WriteString("\n")

	// Missing repositories
	if len(r.MissingRepositories) > 0 {
		sb.WriteString("MISSING REPOSITORIES:\n")
		for _, repo := range r.MissingRepositories {
			sb.WriteString(fmt.Sprintf("  - %s\n", repo))
		}
		sb.WriteString("\n")
	}

	// Missing images (limited to first 100)
	if len(r.MissingImages) > 0 {
		sb.WriteString(fmt.Sprintf("MISSING IMAGES (%d total):\n", len(r.MissingImages)))
		limit := min(100, len(r.MissingImages))
		for i := 0; i < limit; i++ {
			sb.WriteString(fmt.Sprintf("  - %s\n", r.MissingImages[i]))
		}
		if len(r.MissingImages) > limit {
			sb.WriteString(fmt.Sprintf("  ... and %d more\n", len(r.MissingImages)-limit))
		}
		sb.WriteString("\n")
	}

	// Mismatched images (limited to first 50)
	if len(r.MismatchedImages) > 0 {
		sb.WriteString(fmt.Sprintf("MISMATCHED DIGESTS (%d total):\n", len(r.MismatchedImages)))
		limit := min(50, len(r.MismatchedImages))
		for i := 0; i < limit; i++ {
			m := r.MismatchedImages[i]
			sb.WriteString(fmt.Sprintf("  %s\n", m.Reference))
			sb.WriteString(fmt.Sprintf("    source: %s\n", m.SourceDigest))
			sb.WriteString(fmt.Sprintf("    target: %s\n", m.TargetDigest))
		}
		if len(r.MismatchedImages) > limit {
			sb.WriteString(fmt.Sprintf("  ... and %d more\n", len(r.MismatchedImages)-limit))
		}
		sb.WriteString("\n")
	}

	// Layer mismatches
	if len(r.LayerMismatches) > 0 {
		sb.WriteString(fmt.Sprintf("LAYER MISMATCHES (%d total):\n", len(r.LayerMismatches)))
		limit := min(50, len(r.LayerMismatches))
		for i := 0; i < limit; i++ {
			m := r.LayerMismatches[i]
			sb.WriteString(fmt.Sprintf("  %s\n", m.Reference))
			sb.WriteString(fmt.Sprintf("    layer: %s (%s)\n", m.LayerDigest, m.Reason))
		}
		if len(r.LayerMismatches) > limit {
			sb.WriteString(fmt.Sprintf("  ... and %d more\n", len(r.LayerMismatches)-limit))
		}
		sb.WriteString("\n")
	}

	// Per-repository breakdown
	sb.WriteString("REPOSITORY BREAKDOWN:\n")
	sb.WriteString("---------------------\n")

	// Sort repositories for consistent output
	repos := make([]string, 0, len(r.RepositoryDetails))
	for repo := range r.RepositoryDetails {
		repos = append(repos, repo)
	}
	sort.Strings(repos)

	for _, repo := range repos {
		detail := r.RepositoryDetails[repo]
		status := "✓"
		issues := []string{}

		if len(detail.MissingTags) > 0 {
			status = "✗"
			issues = append(issues, fmt.Sprintf("%d missing", len(detail.MissingTags)))
		}

		// Count layer issues
		layerIssues := 0
		deepChecked := 0
		totalLayers := 0
		for _, tagDetail := range detail.TagDetails {
			if tagDetail.DeepChecked {
				deepChecked++
				totalLayers += len(tagDetail.SourceLayers)
				layerIssues += len(tagDetail.MissingLayers)
			}
		}

		if layerIssues > 0 {
			status = "✗"
			issues = append(issues, fmt.Sprintf("%d layer issues", layerIssues))
		}

		repoName := repo
		if repoName == "" {
			repoName = "(root)"
		}

		sb.WriteString(fmt.Sprintf("%s %s: %d/%d tags", status, repoName, detail.MatchedTags, len(detail.SourceTags)))
		if deepChecked > 0 {
			sb.WriteString(fmt.Sprintf(", %d layers checked", totalLayers))
		}
		if len(issues) > 0 {
			sb.WriteString(fmt.Sprintf(" [%s]", strings.Join(issues, ", ")))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// Compare performs a deep comparison between source and target registries
func (c *RegistryComparator) Compare(ctx context.Context) (*ComparisonReport, error) {
	report := &ComparisonReport{
		StartTime:         time.Now(),
		SourceRegistry:    c.sourceRegistry,
		TargetRegistry:    c.targetRegistry,
		RepositoryDetails: make(map[string]*RepositoryComparison),
	}

	// Step 1: Discover all repositories in source
	c.logProgressf("Discovering repositories in source registry...")
	sourceRepos := c.discoverRepositories(c.sourceRegistry, c.sourceRemoteOpts)
	report.SourceRepositories = sourceRepos
	c.logProgressf("Found %d repositories in source", len(sourceRepos))

	// Step 2: Discover all repositories in target
	c.logProgressf("Discovering repositories in target registry...")
	targetRepos := c.discoverRepositories(c.targetRegistry, c.targetRemoteOpts)
	report.TargetRepositories = targetRepos
	c.logProgressf("Found %d repositories in target", len(targetRepos))

	// Step 3: Find missing and extra repositories
	sourceRepoSet := make(map[string]bool)
	for _, r := range sourceRepos {
		sourceRepoSet[r] = true
	}
	targetRepoSet := make(map[string]bool)
	for _, r := range targetRepos {
		targetRepoSet[r] = true
	}

	for _, repo := range sourceRepos {
		if !targetRepoSet[repo] {
			report.MissingRepositories = append(report.MissingRepositories, repo)
		}
	}
	for _, repo := range targetRepos {
		if !sourceRepoSet[repo] {
			report.ExtraRepositories = append(report.ExtraRepositories, repo)
		}
	}

	// Step 4: Compare each repository in detail
	c.logProgressf("Comparing repositories...")
	for i, repoPath := range sourceRepos {
		c.logProgressf("[%d/%d] Comparing %s", i+1, len(sourceRepos), repoPath)

		repoComparison, err := c.compareRepository(repoPath)
		if err != nil {
			c.logProgressf("Warning: failed to compare %s: %v", repoPath, err)
			continue
		}

		report.RepositoryDetails[repoPath] = repoComparison

		// Aggregate stats
		report.TotalSourceImages += len(repoComparison.SourceTags)
		report.TotalTargetImages += len(repoComparison.TargetTags)
		report.SkippedImages += repoComparison.SkippedTags
		report.MatchedImages += repoComparison.MatchedTags

		// Collect missing images
		for _, tag := range repoComparison.MissingTags {
			report.MissingImages = append(report.MissingImages, fmt.Sprintf("%s:%s", repoPath, tag))
		}

		// Collect mismatched images and layer stats
		for tag, detail := range repoComparison.TagDetails {
			if !detail.Match && detail.TargetDigest != "" {
				report.MismatchedImages = append(report.MismatchedImages, ImageMismatch{
					Reference:    fmt.Sprintf("%s:%s", repoPath, tag),
					SourceDigest: detail.SourceDigest,
					TargetDigest: detail.TargetDigest,
				})
			}

			// Aggregate layer stats from deep comparison
			if detail.DeepChecked {
				report.DeepCheckedImages++
				report.TotalSourceLayers += len(detail.SourceLayers)
				report.TotalTargetLayers += len(detail.TargetLayers)
				report.MissingLayers += len(detail.MissingLayers)

				// Count matched layers
				if detail.LayersMatch {
					report.MatchedLayers += len(detail.SourceLayers)
				}

				if !detail.ConfigMatch {
					report.ConfigMismatches++
				}

				// Collect layer mismatches for detailed report
				for _, layer := range detail.MissingLayers {
					report.LayerMismatches = append(report.LayerMismatches, LayerMismatch{
						Reference:   fmt.Sprintf("%s:%s", repoPath, tag),
						LayerDigest: layer,
						Reason:      "missing_in_target",
					})
				}
			}
		}

		// Collect extra images
		for _, tag := range repoComparison.ExtraTags {
			report.ExtraImages = append(report.ExtraImages, fmt.Sprintf("%s:%s", repoPath, tag))
		}
	}

	// Sort results for consistent output
	sort.Strings(report.MissingRepositories)
	sort.Strings(report.MissingImages)
	sort.Strings(report.ExtraImages)
	sort.Slice(report.MismatchedImages, func(i, j int) bool {
		return report.MismatchedImages[i].Reference < report.MismatchedImages[j].Reference
	})

	report.EndTime = time.Now()
	return report, nil
}

// discoverRepositories discovers all repositories by walking known segments
func (c *RegistryComparator) discoverRepositories(registry string, opts []remote.Option) []string {
	var repos []string

	// Root repository
	if c.repositoryExists(registry, opts) {
		repos = append(repos, "")
	}

	// Known segments
	segments := []string{
		internal.InstallSegment,
		internal.InstallStandaloneSegment,
		internal.ReleaseChannelSegment,
	}

	for _, segment := range segments {
		segmentPath := segment
		if c.repositoryExists(path.Join(registry, segmentPath), opts) {
			repos = append(repos, segmentPath)
		}
	}

	// Security segment
	securityDBs := []string{
		internal.SecurityTrivyDBSegment,
		internal.SecurityTrivyBDUSegment,
		internal.SecurityTrivyJavaDBSegment,
		internal.SecurityTrivyChecksSegment,
	}
	for _, db := range securityDBs {
		dbPath := path.Join(internal.SecuritySegment, db)
		if c.repositoryExists(path.Join(registry, dbPath), opts) {
			repos = append(repos, dbPath)
		}
	}

	// Modules - need to discover dynamically
	modulesPath := path.Join(registry, internal.ModulesSegment)
	moduleTags, err := c.listTags(modulesPath, opts)
	if err == nil {
		for _, moduleName := range moduleTags {
			moduleBasePath := path.Join(internal.ModulesSegment, moduleName)

			// Module root
			if c.repositoryExists(path.Join(registry, moduleBasePath), opts) {
				repos = append(repos, moduleBasePath)
			}

			// Module release
			moduleReleasePath := path.Join(moduleBasePath, "release")
			if c.repositoryExists(path.Join(registry, moduleReleasePath), opts) {
				repos = append(repos, moduleReleasePath)
			}
		}
	}

	return repos
}

// repositoryExists checks if a repository exists and has tags
func (c *RegistryComparator) repositoryExists(repo string, opts []remote.Option) bool {
	tags, err := c.listTags(repo, opts)
	return err == nil && len(tags) > 0
}

// compareRepository compares a single repository between source and target
func (c *RegistryComparator) compareRepository(repoPath string) (*RepositoryComparison, error) {
	sourceRepo := c.sourceRegistry
	targetRepo := c.targetRegistry
	if repoPath != "" {
		sourceRepo = path.Join(c.sourceRegistry, repoPath)
		targetRepo = path.Join(c.targetRegistry, repoPath)
	}

	comparison := &RepositoryComparison{
		Repository: repoPath,
		TagDetails: make(map[string]*TagComparison),
	}

	// Get source tags
	allSourceTags, err := c.listTags(sourceRepo, c.sourceRemoteOpts)
	if err != nil {
		return nil, fmt.Errorf("list source tags: %w", err)
	}

	// Filter out tags that are not mirrored by design
	sourceTags := make([]string, 0, len(allSourceTags))
	skippedTags := 0
	for _, tag := range allSourceTags {
		if shouldSkipTag(tag) {
			skippedTags++
			continue
		}
		sourceTags = append(sourceTags, tag)
	}
	comparison.SourceTags = sourceTags
	comparison.SkippedTags = skippedTags

	// Get target tags
	allTargetTags, err := c.listTags(targetRepo, c.targetRemoteOpts)
	if err != nil {
		// Target repo might not exist
		allTargetTags = []string{}
	}

	// Filter target tags too
	targetTags := make([]string, 0, len(allTargetTags))
	for _, tag := range allTargetTags {
		if shouldSkipTag(tag) {
			continue
		}
		targetTags = append(targetTags, tag)
	}
	comparison.TargetTags = targetTags

	// Create sets for comparison
	sourceTagSet := make(map[string]bool)
	for _, t := range sourceTags {
		sourceTagSet[t] = true
	}
	targetTagSet := make(map[string]bool)
	for _, t := range targetTags {
		targetTagSet[t] = true
	}

	// Find missing and extra tags
	for _, tag := range sourceTags {
		if !targetTagSet[tag] {
			comparison.MissingTags = append(comparison.MissingTags, tag)
		}
	}
	for _, tag := range targetTags {
		if !sourceTagSet[tag] {
			comparison.ExtraTags = append(comparison.ExtraTags, tag)
		}
	}

	// Compare images deeply - check manifest, config, and all layers
	var wg sync.WaitGroup
	var mu sync.Mutex
	semaphore := make(chan struct{}, 5) // Limit concurrency (deep comparison is heavier)

	for _, tag := range sourceTags {
		if !targetTagSet[tag] {
			continue
		}

		wg.Add(1)
		go func(tag string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			sourceRef := sourceRepo + ":" + tag
			targetRef := targetRepo + ":" + tag

			// Perform deep comparison
			imgComp, err := c.compareImageDeep(sourceRef, targetRef)
			if err != nil {
				// Fallback to simple digest comparison
				sourceDigest, err1 := c.getDigest(sourceRef, c.sourceRemoteOpts)
				targetDigest, err2 := c.getDigest(targetRef, c.targetRemoteOpts)
				if err1 != nil || err2 != nil {
					return
				}

				tagComp := &TagComparison{
					Tag:          tag,
					SourceDigest: sourceDigest,
					TargetDigest: targetDigest,
					Match:        sourceDigest == targetDigest,
					DeepChecked:  false,
				}

				mu.Lock()
				comparison.TagDetails[tag] = tagComp
				if tagComp.Match {
					comparison.MatchedTags++
				}
				mu.Unlock()
				return
			}

			tagComp := &TagComparison{
				Tag:           tag,
				SourceDigest:  imgComp.SourceDigest,
				TargetDigest:  imgComp.TargetDigest,
				Match:         imgComp.FullMatch,
				SourceConfig:  imgComp.SourceConfig,
				TargetConfig:  imgComp.TargetConfig,
				ConfigMatch:   imgComp.ConfigMatch,
				SourceLayers:  imgComp.SourceLayers,
				TargetLayers:  imgComp.TargetLayers,
				MissingLayers: imgComp.MissingLayers,
				ExtraLayers:   imgComp.ExtraLayers,
				LayersMatch:   imgComp.LayersMatch,
				DeepChecked:   true,
			}

			mu.Lock()
			comparison.TagDetails[tag] = tagComp
			if tagComp.Match {
				comparison.MatchedTags++
			}
			mu.Unlock()
		}(tag)
	}

	wg.Wait()

	sort.Strings(comparison.MissingTags)
	sort.Strings(comparison.ExtraTags)

	return comparison, nil
}

// listTags lists all tags in a repository
func (c *RegistryComparator) listTags(repo string, opts []remote.Option) ([]string, error) {
	repoRef, err := name.NewRepository(repo, c.nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("parse repo %s: %w", repo, err)
	}

	tags, err := remote.List(repoRef, opts...)
	if err != nil {
		return nil, fmt.Errorf("list tags for %s: %w", repo, err)
	}

	return tags, nil
}

// getDigest gets the digest for a specific image reference
func (c *RegistryComparator) getDigest(ref string, opts []remote.Option) (string, error) {
	imgRef, err := name.ParseReference(ref, c.nameOpts...)
	if err != nil {
		return "", fmt.Errorf("parse ref %s: %w", ref, err)
	}

	desc, err := remote.Head(imgRef, opts...)
	if err != nil {
		return "", fmt.Errorf("get digest for %s: %w", ref, err)
	}

	return desc.Digest.String(), nil
}

// getImageInfo gets detailed information about an image including all layer digests
func (c *RegistryComparator) getImageInfo(ref string, opts []remote.Option) (*ImageInfo, error) {
	imgRef, err := name.ParseReference(ref, c.nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("parse ref %s: %w", ref, err)
	}

	// Try to get as an image first
	img, err := remote.Image(imgRef, opts...)
	if err != nil {
		// Might be an index, try that
		idx, idxErr := remote.Index(imgRef, opts...)
		if idxErr != nil {
			return nil, fmt.Errorf("get image %s: %w (also tried index: %v)", ref, err, idxErr)
		}

		// For index, get the digest and list manifests
		digest, err := idx.Digest()
		if err != nil {
			return nil, fmt.Errorf("get index digest: %w", err)
		}

		manifest, err := idx.IndexManifest()
		if err != nil {
			return nil, fmt.Errorf("get index manifest: %w", err)
		}

		info := &ImageInfo{
			Reference: ref,
			Digest:    digest.String(),
			Layers:    make([]string, 0),
		}

		// Collect all manifest digests as "layers" for index
		for _, m := range manifest.Manifests {
			info.Layers = append(info.Layers, m.Digest.String())
			info.TotalSize += m.Size
		}

		return info, nil
	}

	// Get manifest digest
	digest, err := img.Digest()
	if err != nil {
		return nil, fmt.Errorf("get digest: %w", err)
	}

	// Get config digest
	configDigest := ""
	if cfg, err := img.ConfigName(); err == nil {
		configDigest = cfg.String()
	}

	// Get all layer digests
	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("get layers: %w", err)
	}

	info := &ImageInfo{
		Reference:    ref,
		Digest:       digest.String(),
		ConfigDigest: configDigest,
		Layers:       make([]string, 0, len(layers)),
	}

	for _, layer := range layers {
		layerDigest, err := layer.Digest()
		if err != nil {
			continue
		}
		info.Layers = append(info.Layers, layerDigest.String())

		size, err := layer.Size()
		if err == nil {
			info.TotalSize += size
		}
	}

	return info, nil
}

// compareImageDeep performs deep comparison of two images including all layers
func (c *RegistryComparator) compareImageDeep(sourceRef, targetRef string) (*ImageComparison, error) {
	sourceInfo, err := c.getImageInfo(sourceRef, c.sourceRemoteOpts)
	if err != nil {
		return nil, fmt.Errorf("get source image info: %w", err)
	}

	targetInfo, err := c.getImageInfo(targetRef, c.targetRemoteOpts)
	if err != nil {
		return nil, fmt.Errorf("get target image info: %w", err)
	}

	comparison := &ImageComparison{
		Reference:     sourceRef,
		SourceDigest:  sourceInfo.Digest,
		TargetDigest:  targetInfo.Digest,
		DigestMatch:   sourceInfo.Digest == targetInfo.Digest,
		SourceLayers:  sourceInfo.Layers,
		TargetLayers:  targetInfo.Layers,
		MissingLayers: make([]string, 0),
		ExtraLayers:   make([]string, 0),
	}

	// Compare config digests
	if sourceInfo.ConfigDigest != "" && targetInfo.ConfigDigest != "" {
		comparison.ConfigMatch = sourceInfo.ConfigDigest == targetInfo.ConfigDigest
		comparison.SourceConfig = sourceInfo.ConfigDigest
		comparison.TargetConfig = targetInfo.ConfigDigest
	} else {
		comparison.ConfigMatch = true // Skip if not available
	}

	// Compare layers
	targetLayerSet := make(map[string]bool)
	for _, l := range targetInfo.Layers {
		targetLayerSet[l] = true
	}

	sourceLayerSet := make(map[string]bool)
	for _, l := range sourceInfo.Layers {
		sourceLayerSet[l] = true
		if !targetLayerSet[l] {
			comparison.MissingLayers = append(comparison.MissingLayers, l)
		}
	}

	for _, l := range targetInfo.Layers {
		if !sourceLayerSet[l] {
			comparison.ExtraLayers = append(comparison.ExtraLayers, l)
		}
	}

	comparison.LayersMatch = len(comparison.MissingLayers) == 0 && len(comparison.ExtraLayers) == 0
	comparison.FullMatch = comparison.DigestMatch && comparison.ConfigMatch && comparison.LayersMatch

	return comparison, nil
}

// ImageComparison holds detailed comparison of a single image
type ImageComparison struct {
	Reference     string
	SourceDigest  string
	TargetDigest  string
	DigestMatch   bool
	SourceConfig  string
	TargetConfig  string
	ConfigMatch   bool
	SourceLayers  []string
	TargetLayers  []string
	MissingLayers []string
	ExtraLayers   []string
	LayersMatch   bool
	FullMatch     bool
}
