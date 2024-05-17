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

package pull

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/bundle"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/contexts"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/gostsums"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/images"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/layouts"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/manifests"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/releases"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/log"
)

const (
	deckhouseRegistryHost     = "registry.deckhouse.io"
	enterpriseEditionRepoPath = "/deckhouse/ee"

	enterpriseEditionRepo = deckhouseRegistryHost + enterpriseEditionRepoPath
)

var pullLong = templates.LongDesc(`
Download Deckhouse Kubernetes Platform distribution to the local filesystem.
		
This command downloads the Deckhouse Kubernetes Platform distribution bundle 
containing specific platform releases and it's modules, 
to be pushed into the air-gapped container registry at a later time.

For more information on how to use it, consult the docs at 
https://deckhouse.io/documentation/v1/deckhouse-faq.html#manually-uploading-images-to-an-air-gapped-registry

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	pullCmd := &cobra.Command{
		Use:           "pull <images-bundle-path>",
		Short:         "Copy Deckhouse Kubernetes Platform distribution to the local filesystem",
		Long:          pullLong,
		ValidArgs:     []string{"images-bundle-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE:          pull,
		PostRunE: func(_ *cobra.Command, _ []string) error {
			return os.RemoveAll(TempDir)
		},
	}

	addFlags(pullCmd.Flags())
	logs.AddFlags(pullCmd.Flags())
	return pullCmd
}

var (
	TempDir = filepath.Join(os.TempDir(), "mirror")

	Insecure      bool
	TLSSkipVerify bool

	ImagesBundlePath        string
	ImagesBundleChunkSizeGB int64

	minVersionString string
	MinVersion       *semver.Version

	specificReleaseString string
	SpecificRelease       *semver.Version

	SourceRegistryRepo     = enterpriseEditionRepo // Fallback to EE if nothing was given as source.
	SourceRegistryLogin    string
	SourceRegistryPassword string
	DeckhouseLicenseToken  string

	DoGOSTDigest            bool
	DontContinuePartialPull bool
	NoModules               bool
)

func buildPullContext() *contexts.PullContext {
	mirrorCtx := &contexts.PullContext{
		BaseContext: contexts.BaseContext{
			Insecure:              Insecure,
			SkipTLSVerification:   TLSSkipVerify,
			DeckhouseRegistryRepo: SourceRegistryRepo,
			RegistryAuth:          getSourceRegistryAuthProvider(),
			BundlePath:            ImagesBundlePath,
			UnpackedImagesPath: filepath.Join(
				TempDir,
				"pull",
				fmt.Sprintf("%x", md5.Sum([]byte(SourceRegistryRepo))),
			),
		},

		BundleChunkSize: ImagesBundleChunkSizeGB * 1024 * 1024 * 1024,

		DoGOSTDigests:   DoGOSTDigest,
		SkipModulesPull: NoModules,
		SpecificVersion: SpecificRelease,
		MinVersion:      MinVersion,
	}
	return mirrorCtx
}

func pull(_ *cobra.Command, _ []string) error {
	mirrorCtx := buildPullContext()

	if DontContinuePartialPull || lastPullWasTooLongAgoToRetry(mirrorCtx) {
		if err := os.RemoveAll(mirrorCtx.UnpackedImagesPath); err != nil {
			return fmt.Errorf("Cleanup last unfinished pull data: %w", err)
		}
	}

	if err := auth.ValidateReadAccessForImage(
		mirrorCtx.DeckhouseRegistryRepo+":alpha",
		mirrorCtx.RegistryAuth,
		mirrorCtx.Insecure,
		mirrorCtx.SkipTLSVerification,
	); err != nil {
		if os.Getenv("MIRROR_BYPASS_ACCESS_CHECKS") != "1" {
			return fmt.Errorf("Source registry access validation failure: %w", err)
		}
	}

	var versionsToMirror []semver.Version
	var err error
	err = log.Process("mirror", "Looking for required Deckhouse releases", func() error {
		if mirrorCtx.SpecificVersion != nil {
			versionsToMirror = append(versionsToMirror, *mirrorCtx.SpecificVersion)
			log.InfoF("Skipped releases lookup as release %v is specifically requested with --release\n", mirrorCtx.SpecificVersion)
			return nil
		}

		versionsToMirror, err = releases.VersionsToMirror(mirrorCtx)
		if err != nil {
			return fmt.Errorf("Find versions to mirror: %w", err)
		}
		log.InfoF("Deckhouse releases to pull: %+v\n", versionsToMirror)
		return nil
	})
	if err != nil {
		return err
	}

	err = log.Process("mirror", "Pull images", func() error {
		return PullDeckhouseToLocalFS(mirrorCtx, versionsToMirror)
	})
	if err != nil {
		return err
	}

	err = log.Process("mirror", "Pack images", func() error {
		return bundle.Pack(mirrorCtx)
	})
	if err != nil {
		return err
	}

	if mirrorCtx.DoGOSTDigests {
		err = log.Process("mirror", "Compute GOST digest", func() error {
			if err = computeGOSTDigest(&mirrorCtx.BaseContext); err != nil {
				return fmt.Errorf("Compute GOST digest: %w", err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	if err = os.RemoveAll(TempDir); err != nil {
		return fmt.Errorf("Cleanup temporary data after mirroring: %w", err)
	}

	return nil
}

func computeGOSTDigest(mirrorCtx *contexts.BaseContext) error {
	bundleDir := filepath.Dir(mirrorCtx.BundlePath)
	catalog, err := os.ReadDir(bundleDir)
	if err != nil {
		return fmt.Errorf("read tar bundle directory: %w", err)
	}
	streams := make([]io.Reader, 0)
	for _, entry := range catalog {
		fileName := entry.Name()
		if !entry.Type().IsRegular() || filepath.Ext(fileName) != ".chunk" {
			continue
		}
		chunkStream, err := os.Open(filepath.Join(bundleDir, fileName))
		if err != nil {
			return fmt.Errorf("open bundle chunk for reading: %w", err)
		}
		defer chunkStream.Close() // nolint // defer in a loop is valid here as we need those streams to survive until everything is calculated at the end of this function
		streams = append(streams, chunkStream)
	}

	bundleStream := io.NopCloser(io.MultiReader(streams...))
	if len(streams) == 0 {
		bundleStream, err = os.Open(mirrorCtx.BundlePath)
		if err != nil {
			return fmt.Errorf("read tar bundle: %w", err)
		}
	}
	defer bundleStream.Close()

	gostDigest, err := gostsums.CalculateBlobGostDigest(bufio.NewReaderSize(bundleStream, 512*1024))
	if err != nil {
		return fmt.Errorf("Calculate GOST Checksum: %w", err)
	}
	if err = os.WriteFile(mirrorCtx.BundlePath+".gostsum", []byte(gostDigest), 0o666); err != nil {
		return fmt.Errorf("Write GOST Checksum: %w", err)
	}
	log.InfoF("Digest: %s\nWritten to %s\n", gostDigest, mirrorCtx.BundlePath+".gostsum")
	return nil
}

func lastPullWasTooLongAgoToRetry(mirrorCtx *contexts.PullContext) bool {
	s, err := os.Lstat(mirrorCtx.UnpackedImagesPath)
	if err != nil {
		return false
	}

	return time.Since(s.ModTime()) > 24*time.Hour
}

func getSourceRegistryAuthProvider() authn.Authenticator {
	if SourceRegistryLogin != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: SourceRegistryLogin,
			Password: SourceRegistryPassword,
		})
	}

	if DeckhouseLicenseToken != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: DeckhouseLicenseToken,
		})
	}

	return authn.Anonymous
}

func PullDeckhouseToLocalFS(
	pullCtx *contexts.PullContext,
	versions []semver.Version,
) error {
	var err error
	modulesData := make([]modules.Module, 0)

	if !pullCtx.SkipModulesPull {
		log.InfoF("Fetching Deckhouse external modules list...\t")
		modulesData, err = modules.GetDeckhouseExternalModules(pullCtx)
		if err != nil {
			return fmt.Errorf("get Deckhouse modules: %w", err)
		}
		log.InfoLn("✅")
	}

	log.InfoF("Creating OCI Image Layouts...\t")
	imageLayouts, err := layouts.CreateOCIImageLayoutsForDeckhouse(pullCtx.UnpackedImagesPath, modulesData)
	if err != nil {
		return fmt.Errorf("create OCI Image Layouts: %w", err)
	}
	log.InfoLn("✅")

	layouts.FillLayoutsWithBasicDeckhouseImages(pullCtx, imageLayouts, versions)
	if err = imageLayouts.TagsResolver.ResolveTagsDigestsForImageLayouts(&pullCtx.BaseContext, imageLayouts); err != nil {
		return fmt.Errorf("Resolve images tags to digests: %w", err)
	}

	if err = layouts.PullInstallers(pullCtx, imageLayouts); err != nil {
		return fmt.Errorf("pull installers: %w", err)
	}

	log.InfoF("Searching for Deckhouse built-in modules digests...\t")
	for imageTag := range imageLayouts.InstallImages {
		digests, err := images.ExtractImageDigestsFromDeckhouseInstaller(pullCtx, imageTag, imageLayouts.Install)
		if err != nil {
			return fmt.Errorf("extract images digests: %w", err)
		}
		maps.Copy(imageLayouts.DeckhouseImages, digests)
	}
	log.InfoLn("✅")

	if err = layouts.PullDeckhouseReleaseChannels(pullCtx, imageLayouts); err != nil {
		return fmt.Errorf("pull release channels: %w", err)
	}

	// We should not generate deckhousereleases.yaml manifest for single-release bundles
	if pullCtx.SpecificVersion == nil {
		log.InfoF("Generating DeckhouseRelease manifests...\t")
		deckhouseReleasesManifestFile := filepath.Join(filepath.Dir(pullCtx.BundlePath), "deckhousereleases.yaml")
		if err = manifests.GenerateDeckhouseReleaseManifestsForVersions(versions, deckhouseReleasesManifestFile, imageLayouts.ReleaseChannel); err != nil {
			return fmt.Errorf("Generate DeckhouseRelease manifests: %w", err)
		}
		log.InfoLn("✅")
	}

	if err = layouts.PullDeckhouseImages(pullCtx, imageLayouts); err != nil {
		return fmt.Errorf("pull Deckhouse: %w", err)
	}

	log.InfoLn("Pulling Trivy vulnerability databases...\n")
	if err = layouts.PullTrivyVulnerabilityDatabasesImages(pullCtx, imageLayouts); err != nil {
		return fmt.Errorf("pull vulnerability database: %w", err)
	}
	log.InfoLn("Trivy vulnerability databases pulled")

	if !pullCtx.SkipModulesPull {
		log.InfoF("Searching for Deckhouse external modules images...\t")
		if err = layouts.FindDeckhouseModulesImages(pullCtx, imageLayouts); err != nil {
			return fmt.Errorf("find Deckhouse modules images: %w", err)
		}
		log.InfoLn("✅")
		if err = layouts.PullModules(pullCtx, imageLayouts); err != nil {
			return fmt.Errorf("pull Deckhouse modules: %w", err)
		}
	}

	return nil
}
