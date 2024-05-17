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

package push

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/bundle"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/contexts"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/errorutil"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/log"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/retry"
)

var pushLong = templates.LongDesc(`
Upload Deckhouse Kubernetes Platform distribution bundle to the third-party registry.

This command pushes the Deckhouse Kubernetes Platform distribution into the specified container registry.

For more information on how to use it, consult the docs at 
https://deckhouse.io/documentation/v1/deckhouse-faq.html#manually-uploading-images-to-an-air-gapped-registry

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	pushCmd := &cobra.Command{
		Use:           "push <images-bundle-path> <registry>",
		Short:         "Copy Deckhouse Kubernetes Platform distribution to the third-party registry",
		Long:          pushLong,
		ValidArgs:     []string{"images-bundle-path", "registry"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE:          push,
		PostRunE: func(_ *cobra.Command, _ []string) error {
			return os.RemoveAll(TempDir)
		},
	}

	addFlags(pushCmd.Flags())
	logs.AddFlags(pushCmd.Flags())
	return pushCmd
}

const (
	deckhouseRegistryHost     = "registry.deckhouse.io"
	enterpriseEditionRepoPath = "/deckhouse/ee"

	enterpriseEditionRepo = deckhouseRegistryHost + enterpriseEditionRepoPath
)

var (
	TempDir = filepath.Join(os.TempDir(), "mirror")

	registry         string
	RegistryHost     string
	RegistryPath     string
	RegistryUsername string
	RegistryPassword string

	SourceRegistryRepo string

	Insecure         bool
	TLSSkipVerify    bool
	ImagesBundlePath string
)

func push(_ *cobra.Command, _ []string) error {
	mirrorCtx := buildPushContext()

	if RegistryUsername != "" {
		mirrorCtx.RegistryAuth = authn.FromConfig(authn.AuthConfig{
			Username: RegistryUsername,
			Password: RegistryPassword,
		})
	}

	defer os.RemoveAll(mirrorCtx.UnpackedImagesPath)

	if err := auth.ValidateWriteAccessForRepo(
		mirrorCtx.RegistryHost+mirrorCtx.RegistryPath,
		mirrorCtx.RegistryAuth,
		mirrorCtx.Insecure,
		mirrorCtx.SkipTLSVerification,
	); err != nil {
		if os.Getenv("MIRROR_BYPASS_ACCESS_CHECKS") != "1" {
			return fmt.Errorf("registry credentials validation failure: %w", err)
		}
	}

	err := log.Process("mirror", "Unpacking Deckhouse bundle", func() error {
		return bundle.Unpack(&mirrorCtx.BaseContext)
	})
	if err != nil {
		return err
	}

	err = log.Process("mirror", "Push Deckhouse images to registry", func() error {
		return PushDeckhouseToRegistry(mirrorCtx)
	})
	if err != nil {
		return err
	}

	return nil
}

func buildPushContext() *contexts.PushContext {
	mirrorCtx := &contexts.PushContext{
		BaseContext: contexts.BaseContext{
			Insecure:              Insecure,
			SkipTLSVerification:   TLSSkipVerify,
			DeckhouseRegistryRepo: SourceRegistryRepo,
			RegistryHost:          RegistryHost,
			RegistryPath:          RegistryPath,
			BundlePath:            ImagesBundlePath,
			UnpackedImagesPath:    filepath.Join(TempDir, time.Now().Format("mirror_tmp_02-01-2006_15-04-05")),
		},
	}
	return mirrorCtx
}

func PushDeckhouseToRegistry(mirrorCtx *contexts.PushContext) error {
	log.InfoF("Find Deckhouse images to push...\t")
	ociLayouts, modulesList, err := findLayoutsToPush(mirrorCtx)
	if err != nil {
		return fmt.Errorf("Find OCI Image Layouts to push: %w", err)
	}
	log.InfoLn("✅")

	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(&mirrorCtx.BaseContext)

	for originalRepo, ociLayout := range ociLayouts {
		log.InfoLn("Mirroring", originalRepo)
		index, err := ociLayout.ImageIndex()
		if err != nil {
			return fmt.Errorf("read image index from %s: %w", ociLayout, err)
		}

		indexManifest, err := index.IndexManifest()
		if err != nil {
			return fmt.Errorf("read index manifest: %w", err)
		}

		repo := strings.Replace(originalRepo, mirrorCtx.DeckhouseRegistryRepo, mirrorCtx.RegistryHost+mirrorCtx.RegistryPath, 1)
		pushCount := 1
		for _, manifest := range indexManifest.Manifests {
			tag := manifest.Annotations["io.deckhouse.image.short_tag"]
			imageRef := repo + ":" + tag

			img, err := index.Image(manifest.Digest)
			if err != nil {
				return fmt.Errorf("read image: %w", err)
			}

			ref, err := name.ParseReference(imageRef, refOpts...)
			if err != nil {
				return fmt.Errorf("parse oci layout reference: %w", err)
			}

			err = retry.NewLoop(
				fmt.Sprintf("[%d / %d] Pushing image %s...", pushCount, len(indexManifest.Manifests), imageRef),
				20,
				3*time.Second,
			).Run(func() error {
				if err = remote.Write(ref, img, remoteOpts...); err != nil {
					if errorutil.IsTrivyMediaTypeNotAllowedError(err) {
						log.WarnLn(errorutil.CustomTrivyMediaTypesWarning)
						os.Exit(1)
					}
					return fmt.Errorf("write %s to registry: %w", ref.String(), err)
				}
				return nil
			})
			if err != nil {
				return err
			}

			pushCount++
		}
		log.InfoF("Repo %s is mirrored ✅\n", originalRepo)
	}

	log.InfoLn("All repositories are mirrored ✅")

	if len(modulesList) == 0 {
		return nil
	}

	log.InfoLn("Pushing modules tags...")
	if err = pushModulesTags(&mirrorCtx.BaseContext, modulesList); err != nil {
		return fmt.Errorf("Push modules tags: %w", err)
	}
	log.InfoF("All modules tags are pushed ✅\n")

	return nil
}

func pushModulesTags(mirrorCtx *contexts.BaseContext, modulesList []string) error {
	if len(modulesList) == 0 {
		return nil
	}

	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(mirrorCtx)
	modulesRepo := path.Join(mirrorCtx.RegistryHost, mirrorCtx.RegistryPath, "modules")
	pushCount := 1
	for _, moduleName := range modulesList {
		log.InfoF("[%d / %d] Pushing module tag for %s...\t", pushCount, len(modulesList), moduleName)

		imageRef, err := name.ParseReference(modulesRepo+":"+moduleName, refOpts...)
		if err != nil {
			return fmt.Errorf("Parse image reference: %w", err)
		}

		img, err := random.Image(32, 1)
		if err != nil {
			return fmt.Errorf("random.Image: %w", err)
		}

		if err = remote.Write(imageRef, img, remoteOpts...); err != nil {
			return fmt.Errorf("Write module index tag: %w", err)
		}
		log.InfoLn("✅")
		pushCount++
	}
	return nil
}

func findLayoutsToPush(mirrorCtx *contexts.PushContext) (map[string]layout.Path, []string, error) {
	deckhouseIndexRef := mirrorCtx.RegistryHost + mirrorCtx.RegistryPath
	installersIndexRef := filepath.Join(deckhouseIndexRef, "install")
	releasesIndexRef := filepath.Join(deckhouseIndexRef, "release-channel")
	trivyDBIndexRef := filepath.Join(deckhouseIndexRef, "security", "trivy-db")
	trivyBDUIndexRef := filepath.Join(deckhouseIndexRef, "security", "trivy-bdu")
	trivyJavaDBIndexRef := filepath.Join(deckhouseIndexRef, "security", "trivy-java-db")

	deckhouseLayoutPath := mirrorCtx.UnpackedImagesPath
	installersLayoutPath := filepath.Join(mirrorCtx.UnpackedImagesPath, "install")
	releasesLayoutPath := filepath.Join(mirrorCtx.UnpackedImagesPath, "release-channel")
	trivyDBLayoutPath := filepath.Join(mirrorCtx.UnpackedImagesPath, "security", "trivy-db")
	trivyBDULayoutPath := filepath.Join(mirrorCtx.UnpackedImagesPath, "security", "trivy-bdu")
	trivyJavaDBLayoutPath := filepath.Join(mirrorCtx.UnpackedImagesPath, "security", "trivy-java-db")

	deckhouseLayout, err := layout.FromPath(deckhouseLayoutPath)
	if err != nil {
		return nil, nil, err
	}
	installersLayout, err := layout.FromPath(installersLayoutPath)
	if err != nil {
		return nil, nil, err
	}
	releasesLayout, err := layout.FromPath(releasesLayoutPath)
	if err != nil {
		return nil, nil, err
	}
	trivyDBLayout, err := layout.FromPath(trivyDBLayoutPath)
	if err != nil {
		return nil, nil, err
	}
	trivyBDULayout, err := layout.FromPath(trivyBDULayoutPath)
	if err != nil {
		return nil, nil, err
	}
	trivyJavaDBLayout, err := layout.FromPath(trivyJavaDBLayoutPath)
	if err != nil {
		return nil, nil, err
	}

	modulesPath := filepath.Join(mirrorCtx.UnpackedImagesPath, "modules")
	ociLayouts := map[string]layout.Path{
		deckhouseIndexRef:   deckhouseLayout,
		installersIndexRef:  installersLayout,
		releasesIndexRef:    releasesLayout,
		trivyDBIndexRef:     trivyDBLayout,
		trivyBDUIndexRef:    trivyBDULayout,
		trivyJavaDBIndexRef: trivyJavaDBLayout,
	}

	modulesNames := make([]string, 0)
	dirs, err := os.ReadDir(modulesPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return ociLayouts, []string{}, nil
	case err != nil:
		return nil, nil, err
	}

	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}

		moduleName := dir.Name()
		modulesNames = append(modulesNames, moduleName)
		moduleRef := filepath.Join(mirrorCtx.RegistryHost+mirrorCtx.RegistryPath, "modules", moduleName)
		moduleReleasesRef := filepath.Join(mirrorCtx.DeckhouseRegistryRepo, "modules", moduleName, "release")
		moduleLayout, err := layout.FromPath(filepath.Join(modulesPath, moduleName))
		if err != nil {
			return nil, nil, fmt.Errorf("create module layout from path: %w", err)
		}
		moduleReleaseLayout, err := layout.FromPath(filepath.Join(modulesPath, moduleName, "release"))
		if err != nil {
			return nil, nil, fmt.Errorf("create module release layout from path: %w", err)
		}
		ociLayouts[moduleRef] = moduleLayout
		ociLayouts[moduleReleasesRef] = moduleReleaseLayout
	}
	return ociLayouts, modulesNames, nil
}
