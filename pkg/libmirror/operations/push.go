package operations

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/errorutil"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
)

func PushDeckhouseToRegistry(mirrorCtx *contexts.PushContext) error {
	logger := mirrorCtx.Logger
	logger.InfoF("Looking for Deckhouse images to push")
	ociLayouts, modulesList, err := findLayoutsToPush(mirrorCtx)
	if err != nil {
		return fmt.Errorf("Find OCI Image Layouts to push: %w", err)
	}

	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(&mirrorCtx.BaseContext)

	for originalRepo, ociLayout := range ociLayouts {
		logger.InfoLn("Mirroring", originalRepo)
		index, err := ociLayout.ImageIndex()
		if err != nil {
			return fmt.Errorf("read image index from %s: %w", ociLayout, err)
		}

		indexManifest, err := index.IndexManifest()
		if err != nil {
			return fmt.Errorf("read index manifest: %w", err)
		}

		if len(indexManifest.Manifests) == 0 {
			logger.InfoLn("Skipped repo", originalRepo, "as it contains no images")
			continue
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

			err = retry.RunTask(
				logger,
				fmt.Sprintf("[%d / %d] Pushing image %s ", pushCount, len(indexManifest.Manifests), imageRef),
				task.WithConstantRetries(19, 3*time.Second, func() error {
					if err = remote.Write(ref, img, remoteOpts...); err != nil {
						if errorutil.IsTrivyMediaTypeNotAllowedError(err) {
							logger.WarnLn(errorutil.CustomTrivyMediaTypesWarning)
							os.Exit(1)
						}
						return fmt.Errorf("write %s to registry: %w", ref.String(), err)
					}
					return nil
				}))
			if err != nil {
				return err
			}

			pushCount++
		}
		logger.InfoF("✅Repo %s is mirrored", originalRepo)
	}

	logger.InfoLn("✅All repositories are mirrored")

	if len(modulesList) == 0 {
		return nil
	}

	logger.InfoLn("Pushing modules tags")
	if err = pushModulesTags(&mirrorCtx.BaseContext, modulesList); err != nil {
		return fmt.Errorf("Push modules tags: %w", err)
	}
	logger.InfoF("✅All modules tags are pushed")

	return nil
}

func pushModulesTags(mirrorCtx *contexts.BaseContext, modulesList []string) error {
	if len(modulesList) == 0 {
		return nil
	}

	logger := mirrorCtx.Logger

	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(mirrorCtx)
	modulesRepo := path.Join(mirrorCtx.RegistryHost, mirrorCtx.RegistryPath, "modules")
	pushCount := 1
	for _, moduleName := range modulesList {
		logger.InfoF("[%d / %d] Pushing module tag for %s", pushCount, len(modulesList), moduleName)

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
		logger.InfoLn("✅")
		pushCount++
	}
	return nil
}

func findLayoutsToPush(mirrorCtx *contexts.PushContext) (map[string]layout.Path, []string, error) {
	deckhouseIndexRef := mirrorCtx.RegistryHost + mirrorCtx.RegistryPath
	installersIndexRef := path.Join(deckhouseIndexRef, "install")
	releasesIndexRef := path.Join(deckhouseIndexRef, "release-channel")
	trivyDBIndexRef := path.Join(deckhouseIndexRef, "security", "trivy-db")
	trivyBDUIndexRef := path.Join(deckhouseIndexRef, "security", "trivy-bdu")
	trivyJavaDBIndexRef := path.Join(deckhouseIndexRef, "security", "trivy-java-db")

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
		moduleRef := path.Join(mirrorCtx.RegistryHost+mirrorCtx.RegistryPath, "modules", moduleName)
		moduleReleasesRef := path.Join(mirrorCtx.DeckhouseRegistryRepo, "modules", moduleName, "release")
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
