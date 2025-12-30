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
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	d8internal "github.com/deckhouse/deckhouse-cli/internal"
)

type DigestVerifier struct {
	sourceReader *SourceReader
	targetReg    string
	targetAuth   authn.Authenticator
	targetOpts   []remote.Option
	progressFn   func(string)
}

func NewDigestVerifier(
	sourceReader *SourceReader,
	targetReg string,
	targetAuth authn.Authenticator,
	tlsSkipVerify bool,
) *DigestVerifier {
	opts := []remote.Option{remote.WithAuth(targetAuth)}
	if tlsSkipVerify {
		opts = append(opts, remote.WithTransport(InsecureTransport()))
	}

	return &DigestVerifier{
		sourceReader: sourceReader,
		targetReg:    targetReg,
		targetAuth:   targetAuth,
		targetOpts:   opts,
	}
}

func (v *DigestVerifier) SetProgressCallback(fn func(string)) {
	v.progressFn = fn
	v.sourceReader.SetProgressCallback(fn)
}

func (v *DigestVerifier) logProgressf(format string, args ...interface{}) {
	if v.progressFn != nil {
		v.progressFn(fmt.Sprintf(format, args...))
	}
}

type VerificationResult struct {
	StartTime time.Time
	EndTime   time.Time

	ExpectedDigests []string
	ExpectedAttTags []string
	ReleaseChannels []ReleaseChannelInfo
	Versions        []string

	FoundDigests   []string
	MissingDigests []string
	FoundAttTags   []string
	MissingAttTags []string

	ModulesExpected int
	ModulesFound    int
	ModulesMissing  []string

	SecurityExpected int
	SecurityFound    int
	SecurityMissing  []string

	Errors []string

	TotalExpected int
	TotalFound    int
	TotalMissing  int
}

func (r *VerificationResult) IsSuccess() bool {
	return len(r.MissingDigests) == 0 &&
		len(r.MissingAttTags) == 0 &&
		len(r.ModulesMissing) == 0 &&
		len(r.SecurityMissing) == 0
}

func (r *VerificationResult) Summary() string {
	var sb strings.Builder

	sb.WriteString("VERIFICATION SUMMARY\n")
	sb.WriteString("====================\n\n")

	sb.WriteString(fmt.Sprintf("Duration: %v\n", r.EndTime.Sub(r.StartTime).Round(time.Second)))
	sb.WriteString(fmt.Sprintf("Release channels: %d\n", len(r.ReleaseChannels)))
	sb.WriteString(fmt.Sprintf("Versions: %d\n", len(r.Versions)))
	sb.WriteString("\n")

	sb.WriteString("EXPECTED FROM SOURCE:\n")
	sb.WriteString(fmt.Sprintf("  Platform digests: %d\n", len(r.ExpectedDigests)))
	sb.WriteString(fmt.Sprintf("  Attestation tags (.att): %d\n", len(r.ExpectedAttTags)))
	if r.ModulesExpected > 0 {
		sb.WriteString(fmt.Sprintf("  Modules: %d\n", r.ModulesExpected))
	}
	if r.SecurityExpected > 0 {
		sb.WriteString(fmt.Sprintf("  Security databases: %d\n", r.SecurityExpected))
	}
	sb.WriteString("\n")

	sb.WriteString("VERIFICATION RESULTS:\n")
	sb.WriteString(fmt.Sprintf("  ✓ Platform digests: %d / %d\n", len(r.FoundDigests), len(r.ExpectedDigests)))
	sb.WriteString(fmt.Sprintf("  ✗ Missing digests: %d\n", len(r.MissingDigests)))
	sb.WriteString(fmt.Sprintf("  ✓ Attestation tags: %d / %d\n", len(r.FoundAttTags), len(r.ExpectedAttTags)))
	sb.WriteString(fmt.Sprintf("  ✗ Missing .att tags: %d\n", len(r.MissingAttTags)))
	if r.ModulesExpected > 0 {
		sb.WriteString(fmt.Sprintf("  ✓ Modules: %d / %d\n", r.ModulesFound, r.ModulesExpected))
		if len(r.ModulesMissing) > 0 {
			sb.WriteString(fmt.Sprintf("  ✗ Missing modules: %d\n", len(r.ModulesMissing)))
		}
	}
	if r.SecurityExpected > 0 {
		sb.WriteString(fmt.Sprintf("  ✓ Security databases: %d / %d\n", r.SecurityFound, r.SecurityExpected))
		if len(r.SecurityMissing) > 0 {
			sb.WriteString(fmt.Sprintf("  ✗ Missing security: %d\n", len(r.SecurityMissing)))
		}
	}
	sb.WriteString("\n")

	if r.IsSuccess() {
		sb.WriteString("STATUS: ✓ PASSED - All expected images found in target\n")
	} else {
		sb.WriteString("STATUS: ✗ FAILED - Some images missing in target\n")
	}

	return sb.String()
}

func (r *VerificationResult) DetailedReport() string {
	var sb strings.Builder

	sb.WriteString(r.Summary())
	sb.WriteString("\n")

	sb.WriteString("RELEASE CHANNELS:\n")
	for _, ch := range r.ReleaseChannels {
		sb.WriteString(fmt.Sprintf("  %s -> %s\n", ch.Channel, ch.Version))
	}
	sb.WriteString("\n")

	if len(r.MissingDigests) > 0 {
		sb.WriteString("MISSING DIGESTS:\n")
		for _, d := range r.MissingDigests {
			sb.WriteString(fmt.Sprintf("  - %s\n", d))
		}
		sb.WriteString("\n")
	}

	if len(r.MissingAttTags) > 0 {
		sb.WriteString("MISSING ATTESTATION TAGS:\n")
		for _, t := range r.MissingAttTags {
			sb.WriteString(fmt.Sprintf("  - %s\n", t))
		}
		sb.WriteString("\n")
	}

	if len(r.ModulesMissing) > 0 {
		sb.WriteString("MISSING MODULES:\n")
		for _, m := range r.ModulesMissing {
			sb.WriteString(fmt.Sprintf("  - %s\n", m))
		}
		sb.WriteString("\n")
	}

	if len(r.SecurityMissing) > 0 {
		sb.WriteString("MISSING SECURITY DATABASES:\n")
		for _, s := range r.SecurityMissing {
			sb.WriteString(fmt.Sprintf("  - %s\n", s))
		}
		sb.WriteString("\n")
	}

	if len(r.Errors) > 0 {
		sb.WriteString("ERRORS:\n")
		for _, e := range r.Errors {
			sb.WriteString(fmt.Sprintf("  - %s\n", e))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func (v *DigestVerifier) VerifyPlatform(ctx context.Context, deckhouseTag string) (*VerificationResult, error) {
	result := &VerificationResult{
		StartTime: time.Now(),
	}

	v.logProgressf("Reading release channels from source...")
	var channels []ReleaseChannelInfo
	var err error

	if deckhouseTag != "" {
		v.logProgressf("  Using specified tag: %s", deckhouseTag)
		channels = []ReleaseChannelInfo{{Channel: deckhouseTag, Version: deckhouseTag}}
	} else {
		channels, err = v.sourceReader.ReadReleaseChannels(ctx)
		if err != nil {
			return nil, fmt.Errorf("read release channels: %w", err)
		}
	}
	result.ReleaseChannels = channels
	v.logProgressf("  Found %d release channels", len(channels))

	v.logProgressf("Reading platform digests from install images...")
	platformDigests, err := v.sourceReader.ReadPlatformDigests(ctx, channels)
	if err != nil {
		return nil, fmt.Errorf("read platform digests: %w", err)
	}

	result.Versions = platformDigests.Versions
	result.ExpectedDigests = platformDigests.ImageDigests
	v.logProgressf("  Found %d unique digests across %d versions", len(result.ExpectedDigests), len(result.Versions))

	if len(result.ExpectedDigests) == 0 {
		tag := "unknown"
		if len(channels) > 0 {
			tag = channels[0].Version
		}
		return nil, fmt.Errorf("found 0 platform digests - verification cannot proceed (check install image for tag %s)", tag)
	}

	v.logProgressf("Finding .att tags for expected digests...")
	allAttTags := v.getAttTagsFromSource(ctx)
	result.ExpectedAttTags = v.filterAttTagsForDigests(allAttTags, result.ExpectedDigests)
	v.logProgressf("  Found %d .att tags in source (%d total, %d match our digests)",
		len(result.ExpectedAttTags), len(allAttTags), len(result.ExpectedAttTags))

	v.logProgressf("Verifying digests in target registry...")
	v.verifyDigests(ctx, result)
	v.logProgressf("  Found: %d, Missing: %d", len(result.FoundDigests), len(result.MissingDigests))

	v.logProgressf("Verifying .att tags in target registry...")
	v.verifyAttTags(ctx, result)
	v.logProgressf("  Found: %d, Missing: %d", len(result.FoundAttTags), len(result.MissingAttTags))

	result.EndTime = time.Now()
	result.TotalExpected = len(result.ExpectedDigests) + len(result.ExpectedAttTags)
	result.TotalFound = len(result.FoundDigests) + len(result.FoundAttTags)
	result.TotalMissing = len(result.MissingDigests) + len(result.MissingAttTags)

	return result, nil
}


func (v *DigestVerifier) VerifyModules(ctx context.Context, moduleNames []string) (*VerificationResult, error) {
	result := &VerificationResult{
		StartTime: time.Now(),
	}

	v.logProgressf("Getting module list...")
	var modules []string
	var err error

	if len(moduleNames) > 0 && moduleNames[0] != "" {
		modules = moduleNames
	} else {
		modules, err = v.sourceReader.ReadModulesList(ctx)
		if err != nil {
			return nil, fmt.Errorf("read modules list: %w", err)
		}
	}
	v.logProgressf("  Found %d modules to verify", len(modules))

	result.ModulesExpected = len(modules)

	releaseChannels := d8internal.GetAllDefaultReleaseChannels()
	sourceOpts := v.sourceReader.RemoteOpts()

	v.logProgressf("Verifying modules...")

	for _, moduleName := range modules {
		v.logProgressf("  Checking module: %s", moduleName)

		sourceReleaseRepo := path.Join(v.sourceReader.Registry(), d8internal.ModulesSegment, moduleName, "release")
		targetReleaseRepo := path.Join(v.targetReg, d8internal.ModulesSegment, moduleName, "release")

		targetRef, err := name.ParseReference(targetReleaseRepo + ":latest")
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("module %s: invalid target reference: %v", moduleName, err))
			continue
		}

		targetOpts := append(v.targetOpts, remote.WithContext(ctx))
		targetTags, err := remote.List(targetRef.Context(), targetOpts...)
		if err != nil {
			v.logProgressf("    Target: no tags found (module not mirrored)")
			result.ModulesMissing = append(result.ModulesMissing, moduleName)
			continue
		}

		// Filter to only release channels
		targetChannels := []string{}
		for _, tag := range targetTags {
			for _, channel := range releaseChannels {
				if tag == channel {
					targetChannels = append(targetChannels, tag)
					break
				}
			}
		}

		if len(targetChannels) == 0 {
			v.logProgressf("    Target: no release channels found (module not properly mirrored)")
			result.ModulesMissing = append(result.ModulesMissing, moduleName)
			continue
		}

		v.logProgressf("    Target has %d channels: %v", len(targetChannels), targetChannels)

		matchedChannels := []string{}
		mismatchedChannels := []string{}
		sourceNotFoundChannels := []string{}

		for _, channel := range targetChannels {
			targetTagRef := targetReleaseRepo + ":" + channel
			targetImgRef, err := name.ParseReference(targetTagRef)
			if err != nil {
				continue
			}

			targetDesc, err := remote.Head(targetImgRef, targetOpts...)
			if err != nil {
				continue // Already listed, should exist
			}
			targetDigest := targetDesc.Digest.String()

			sourceTagRef := sourceReleaseRepo + ":" + channel
			sourceImgRef, err := name.ParseReference(sourceTagRef)
			if err != nil {
				sourceNotFoundChannels = append(sourceNotFoundChannels, channel)
				continue
			}

			sourceOptsWithCtx := append(sourceOpts, remote.WithContext(ctx))
			sourceDesc, err := remote.Head(sourceImgRef, sourceOptsWithCtx...)
			if err != nil {
				// Channel exists in target but not in source - might be removed upstream
				v.logProgressf("    ⚠ %s: exists in target but not in source (may be removed upstream)", channel)
				sourceNotFoundChannels = append(sourceNotFoundChannels, channel)
				continue
			}
			sourceDigest := sourceDesc.Digest.String()

			if sourceDigest != targetDigest {
				v.logProgressf("    ✗ %s: DIGEST MISMATCH!", channel)
				v.logProgressf("        Source: %s", sourceDigest)
				v.logProgressf("        Target: %s", targetDigest)
				mismatchedChannels = append(mismatchedChannels, channel)
				result.Errors = append(result.Errors,
					fmt.Sprintf("module %s/%s: digest mismatch (source=%s, target=%s)",
						moduleName, channel, sourceDigest[:19], targetDigest[:19]))
			} else {
				v.logProgressf("    ✓ %s: digest match %s", channel, sourceDigest[:19])
				matchedChannels = append(matchedChannels, channel)
			}
		}

		if len(mismatchedChannels) > 0 {
			v.logProgressf("    Result: %d matched, %d MISMATCHED, %d source-not-found",
				len(matchedChannels), len(mismatchedChannels), len(sourceNotFoundChannels))
			result.ModulesFound++
		} else if len(matchedChannels) > 0 {
			v.logProgressf("    ✓ All %d channels verified with matching digests", len(matchedChannels))
			result.ModulesFound++
		} else if len(sourceNotFoundChannels) > 0 {
			v.logProgressf("    ⚠ All %d channels exist only in target (removed from source?)", len(sourceNotFoundChannels))
			result.ModulesFound++
		}
	}

	result.EndTime = time.Now()
	return result, nil
}

func (v *DigestVerifier) VerifySecurity(ctx context.Context) (*VerificationResult, error) {
	result := &VerificationResult{
		StartTime: time.Now(),
	}

	expectedTags := map[string]string{
		d8internal.SecurityTrivyDBSegment:     "2",
		d8internal.SecurityTrivyBDUSegment:    "1",
		d8internal.SecurityTrivyJavaDBSegment: "1",
		d8internal.SecurityTrivyChecksSegment: "0",
	}

	result.SecurityExpected = len(expectedTags)
	sourceOpts := v.sourceReader.RemoteOpts()
	sourceReg := v.sourceReader.Registry()

	v.logProgressf("Verifying security databases...")

	for db, expectedTag := range expectedTags {
		v.logProgressf("  Checking %s:%s...", db, expectedTag)

		sourceRef := path.Join(sourceReg, d8internal.SecuritySegment, db) + ":" + expectedTag
		sourceImgRef, err := name.ParseReference(sourceRef)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("db %s: invalid source reference: %v", db, err))
			result.SecurityMissing = append(result.SecurityMissing, db)
			continue
		}

		sourceOptsWithCtx := append(sourceOpts, remote.WithContext(ctx))
		sourceDesc, err := remote.Head(sourceImgRef, sourceOptsWithCtx...)
		if err != nil {
			v.logProgressf("    ⚠ Cannot read source %s:%s: %v (skipping)", db, expectedTag, err)
			result.SecurityExpected--
			continue
		}
		sourceDigest := sourceDesc.Digest.String()
		v.logProgressf("    Source digest: %s", sourceDigest[:19])

		targetRef := path.Join(v.targetReg, d8internal.SecuritySegment, db) + ":" + expectedTag
		targetImgRef, err := name.ParseReference(targetRef)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("db %s: invalid target reference: %v", db, err))
			result.SecurityMissing = append(result.SecurityMissing, db)
			continue
		}

		targetOpts := append(v.targetOpts, remote.WithContext(ctx))
		targetDesc, err := remote.Head(targetImgRef, targetOpts...)
		if err != nil {
			v.logProgressf("    ✗ %s:%s NOT FOUND in target", db, expectedTag)
			result.SecurityMissing = append(result.SecurityMissing, db)
			continue
		}
		targetDigest := targetDesc.Digest.String()

		if sourceDigest != targetDigest {
			v.logProgressf("    ✗ %s:%s DIGEST MISMATCH!", db, expectedTag)
			v.logProgressf("        Source: %s", sourceDigest)
			v.logProgressf("        Target: %s", targetDigest)
			result.Errors = append(result.Errors,
				fmt.Sprintf("db %s: digest mismatch (source=%s, target=%s)",
					db, sourceDigest[:19], targetDigest[:19]))
			result.SecurityMissing = append(result.SecurityMissing, db)
			continue
		}

		v.logProgressf("    ✓ %s:%s digest match %s", db, expectedTag, sourceDigest[:19])
		result.SecurityFound++
	}

	result.EndTime = time.Now()
	return result, nil
}

type verifyItem struct {
	ref    string
	onErr  func(string, error)
	onFound func(string)
	onMissing func(string)
}

func (v *DigestVerifier) verifyInParallel(ctx context.Context, items []verifyItem) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	sem := make(chan struct{}, 10)

	for _, item := range items {
		wg.Add(1)
		go func(it verifyItem) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			imgRef, err := name.ParseReference(it.ref)
			if err != nil {
				mu.Lock()
				it.onErr(it.ref, err)
				mu.Unlock()
				return
			}

			opts := append(v.targetOpts, remote.WithContext(ctx))
			_, err = remote.Head(imgRef, opts...)
			mu.Lock()
			if err != nil {
				it.onMissing(it.ref)
			} else {
				it.onFound(it.ref)
			}
			mu.Unlock()
		}(item)
	}

	wg.Wait()
}

func (v *DigestVerifier) verifyDigests(ctx context.Context, result *VerificationResult) {
	items := make([]verifyItem, 0, len(result.ExpectedDigests))
	for _, digest := range result.ExpectedDigests {
		digest := digest // capture loop variable
		ref := v.targetReg + "@" + digest
		items = append(items, verifyItem{
			ref: ref,
			onErr: func(ref string, err error) {
				result.Errors = append(result.Errors, fmt.Sprintf("invalid digest ref %s: %v", digest, err))
			},
			onFound: func(ref string) {
				result.FoundDigests = append(result.FoundDigests, digest)
			},
			onMissing: func(ref string) {
				result.MissingDigests = append(result.MissingDigests, digest)
			},
		})
	}
	v.verifyInParallel(ctx, items)
}

func (v *DigestVerifier) getAttTagsFromSource(ctx context.Context) []string {
	sourceReg := v.sourceReader.Registry()
	sourceOpts := append(v.sourceReader.RemoteOpts(), remote.WithContext(ctx))

	ref, err := name.ParseReference(sourceReg + ":latest")
	if err != nil {
		v.logProgressf("  Warning: failed to parse source registry: %v", err)
		return nil
	}

	repo := ref.Context()
	tags, err := remote.List(repo, sourceOpts...)
	if err != nil {
		v.logProgressf("  Warning: failed to list source tags: %v", err)
		return nil
	}

	var attTags []string
	for _, tag := range tags {
		if strings.HasSuffix(tag, ".att") {
			attTags = append(attTags, tag)
		}
	}

	return attTags
}

func (v *DigestVerifier) filterAttTagsForDigests(attTags []string, digests []string) []string {
	expectedPrefixes := make(map[string]bool)
	for _, digest := range digests {
		if strings.HasPrefix(digest, "sha256:") {
			hash := strings.TrimPrefix(digest, "sha256:")
			prefix := "sha256-" + hash
			expectedPrefixes[prefix] = true
		} else {
			digestPreview := digest
			if len(digest) > 20 {
				digestPreview = digest[:20]
			}
			v.logProgressf("  Warning: non-sha256 digest found: %s (skipping .att tag matching)", digestPreview)
		}
	}

	var filtered []string
	for _, tag := range attTags {
		if strings.HasSuffix(tag, ".att") {
			prefix := strings.TrimSuffix(tag, ".att")
			if expectedPrefixes[prefix] {
				filtered = append(filtered, tag)
			}
		}
	}

	return filtered
}

func (v *DigestVerifier) verifyAttTags(ctx context.Context, result *VerificationResult) {
	items := make([]verifyItem, 0, len(result.ExpectedAttTags))
	for _, attTag := range result.ExpectedAttTags {
		attTag := attTag // capture loop variable
		ref := v.targetReg + ":" + attTag
		items = append(items, verifyItem{
			ref: ref,
			onErr: func(ref string, err error) {
				result.Errors = append(result.Errors, fmt.Sprintf("invalid att ref %s: %v", attTag, err))
			},
			onFound: func(ref string) {
				result.FoundAttTags = append(result.FoundAttTags, attTag)
			},
			onMissing: func(ref string) {
				result.MissingAttTags = append(result.MissingAttTags, attTag)
			},
		})
	}
	v.verifyInParallel(ctx, items)
}

func (v *DigestVerifier) VerifyFull(ctx context.Context, deckhouseTag string, moduleNames []string) (*VerificationResult, error) {
	result := &VerificationResult{
		StartTime: time.Now(),
	}

	v.logProgressf("=== PLATFORM VERIFICATION ===")
	platformResult, err := v.VerifyPlatform(ctx, deckhouseTag)
	if err != nil {
		return nil, fmt.Errorf("platform verification: %w", err)
	}
	mergeResults(result, platformResult)

	v.logProgressf("\n=== MODULES VERIFICATION ===")
	modulesResult, err := v.VerifyModules(ctx, moduleNames)
	if err != nil {
		return nil, fmt.Errorf("modules verification: %w", err)
	}
	mergeResults(result, modulesResult)

	v.logProgressf("\n=== SECURITY VERIFICATION ===")
	securityResult, err := v.VerifySecurity(ctx)
	if err != nil {
		return nil, fmt.Errorf("security verification: %w", err)
	}
	mergeResults(result, securityResult)

	result.EndTime = time.Now()
	result.TotalExpected = len(result.ExpectedDigests) + len(result.ExpectedAttTags) +
		result.ModulesExpected + result.SecurityExpected
	result.TotalFound = len(result.FoundDigests) + len(result.FoundAttTags) +
		result.ModulesFound + result.SecurityFound
	result.TotalMissing = len(result.MissingDigests) + len(result.MissingAttTags) +
		len(result.ModulesMissing) + len(result.SecurityMissing)

	return result, nil
}

func mergeResults(dst, src *VerificationResult) {
	dst.ExpectedDigests = append(dst.ExpectedDigests, src.ExpectedDigests...)
	dst.ExpectedAttTags = append(dst.ExpectedAttTags, src.ExpectedAttTags...)
	dst.ReleaseChannels = append(dst.ReleaseChannels, src.ReleaseChannels...)
	dst.Versions = append(dst.Versions, src.Versions...)
	dst.FoundDigests = append(dst.FoundDigests, src.FoundDigests...)
	dst.MissingDigests = append(dst.MissingDigests, src.MissingDigests...)
	dst.FoundAttTags = append(dst.FoundAttTags, src.FoundAttTags...)
	dst.MissingAttTags = append(dst.MissingAttTags, src.MissingAttTags...)

	dst.ModulesExpected += src.ModulesExpected
	dst.ModulesFound += src.ModulesFound
	dst.ModulesMissing = append(dst.ModulesMissing, src.ModulesMissing...)

	dst.SecurityExpected += src.SecurityExpected
	dst.SecurityFound += src.SecurityFound
	dst.SecurityMissing = append(dst.SecurityMissing, src.SecurityMissing...)

	dst.Errors = append(dst.Errors, src.Errors...)
}

