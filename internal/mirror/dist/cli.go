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

package dist

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/pack"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// cliBundleName is the bundle archive carrying the d8 binary.
const cliBundleName = "deckhouse-cli.tar"

// PullCLI mirrors the d8 binary itself - one version of <root>/deckhouse-cli,
// the repository the in-cluster registry-packages-proxy serves to `d8 dist
// update`. Without it an air-gapped cluster can install the plugins in the
// bundle but never the CLI that runs them.
//
// The newest published stable version is mirrored, or the exact tag pinned
// with --deckhouse-cli-tag. Automatic selection needs the tag listing, so a
// registry that publishes no deckhouse-cli repository, denies access to it, or
// serves no catalog at all (--proxy-registry) yields a warning rather than a
// failed pull - the same treatment the plugins catalog gets. A pinned tag is
// an explicit request, so failing to mirror it fails the pull.
func (svc *Service) PullCLI(ctx context.Context) error {
	svc.cliStats.attempted = true

	tag, skipReason, err := svc.resolveCLITag(ctx)
	if err != nil {
		return err
	}

	if skipReason != "" {
		// Automatic selection: the CLI is simply not on offer here. Say so
		// and move on - the rest of the bundle is unaffected.
		svc.cliStats.skipReason = skipReason
		svc.userLogger.WarnLn("d8 CLI not mirrored: " + skipReason)

		return nil
	}

	ref := svc.cliRef(tag)

	if svc.options.DryRun {
		svc.userLogger.InfoLn("[dry-run] Deckhouse CLI that would be pulled:")
		svc.userLogger.InfoLn("  " + ref)

		svc.cliStats.version = tag

		return nil
	}

	cliLayout, err := svc.layoutForCLI()
	if err != nil {
		return err
	}

	err = retry.RunTask(
		ctx,
		svc.userLogger,
		"Pulling "+ref,
		task.WithConstantRetries(pullRetryAttempts, pullRetryDelay, func(ctx context.Context) error {
			return pullTag(ctx, svc.cliService, cliLayout, tag, ref)
		}))
	if err != nil {
		if svc.options.CLITag != "" {
			return fmt.Errorf("pull deckhouse-cli %s: %w", tag, err)
		}

		// An automatically picked version that cannot be pulled is worth a
		// warning, not a dead bundle: everything else in it is still usable.
		// The transport error goes to the log; the summary gets the phrase.
		svc.logger.Debug(fmt.Sprintf("Pulling deckhouse-cli %s failed: %v", tag, err))

		svc.cliStats.skipReason = "could not be pulled at " + tag
		svc.userLogger.WarnLn("d8 CLI not mirrored: could not be pulled at " + tag)

		return nil
	}

	svc.cliStats.version = tag
	// The image count must be captured before packing: bundle.Pack deletes the
	// layout files as it tars them.
	svc.cliStats.images = regimage.CountManifests([]layout.Path{cliLayout.Path()})

	return svc.packCLI(ctx)
}

// resolveCLITag picks the version to mirror: the pinned tag as given, or the
// newest published stable one.
//
// A registry that simply does not offer the binary is not a failure: it yields
// an empty tag and a short skip reason for the summary, the way an absent
// platform plugin yields a warning. The registry's own error text goes to the
// debug log rather than the summary - a wrapped NAME_UNKNOWN chain names the
// repository the label already names, and buries the one fact the operator
// needs behind four levels of transport detail.
//
// The error is reserved for a pinned version that cannot be resolved: the user
// asked for it by name, so it stops the pull.
func (svc *Service) resolveCLITag(ctx context.Context) (versionTag, string, error) {
	if pinned := svc.options.CLITag; pinned != "" {
		// An explicit version is checked before the pull so a typo fails
		// immediately instead of after the transfer retries are exhausted.
		if err := svc.cliService.CheckImageExists(ctx, pinned); err != nil {
			return "", "", fmt.Errorf("deckhouse-cli %s: %w", pinned, err)
		}

		return pinned, "", nil
	}

	if svc.options.ProxyRegistry {
		return "", "no version listing over a proxy registry; pin it with --deckhouse-cli-tag <version>", nil
	}

	tags, err := svc.cliService.ListTags(ctx)
	if err != nil {
		// Most registries answer this way when the repository was never
		// published, or when the license has no access to it.
		svc.logger.Debug(fmt.Sprintf("Listing tags of %s failed: %v", svc.cliService.GetRoot(), err))

		return "", "not available in this registry", nil
	}

	versions := stableVersions(sortedSemverDesc(tags))
	if len(versions) == 0 {
		return "", "no published versions", nil
	}

	return versions[0].Original(), "", nil
}

func (svc *Service) packCLI(ctx context.Context) error {
	return svc.userLogger.Process(fmt.Sprintf("Pack %s", cliBundleName), func() error {
		// The tar prefix places the layout at deckhouse-cli inside the bundle -
		// the path mirror push uploads verbatim and the registry-packages-proxy
		// expects on the target side.
		cliDir := filepath.Join(svc.workingDir, internal.D8CLISegment)

		return pack.Bundle(ctx, svc.options.BundleDir, cliBundleName, svc.options.BundleChunkSize, func(w io.Writer) error {
			return bundle.PackWithPrefix(ctx, cliDir, internal.D8CLISegment, w)
		})
	})
}

func (svc *Service) layoutForCLI() (*regimage.ImageLayout, error) {
	if svc.cliLayout != nil {
		return svc.cliLayout, nil
	}

	cliLayout, err := regimage.NewImageLayout(filepath.Join(svc.workingDir, internal.D8CLISegment))
	if err != nil {
		return nil, fmt.Errorf("create layout for deckhouse-cli: %w", err)
	}

	svc.cliLayout = cliLayout

	return cliLayout, nil
}

// cliRef is the full registry reference of one CLI version, e.g.
// "registry.deckhouse.io/deckhouse/deckhouse-cli:v0.13.1".
func (svc *Service) cliRef(tag versionTag) string {
	return svc.cliService.GetRoot() + ":" + tag
}
