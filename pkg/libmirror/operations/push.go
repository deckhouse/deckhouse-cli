package operations

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

func PushDeckhouseToRegistry(mirrorCtx *contexts.PushContext) error {
	return PushDeckhouseToRegistryContext(context.Background(), mirrorCtx)
}

func PushDeckhouseToRegistryContext(ctx context.Context, mirrorCtx *contexts.PushContext) error {
	logger := mirrorCtx.Logger
	logger.InfoF("Looking for Deckhouse images to push")
	ociLayouts, modulesList, err := findLayoutsToPush(ctx, mirrorCtx)
	if err != nil {
		return fmt.Errorf("Find OCI Image Layouts to push: %w", err)
	}

	for repo, ociLayout := range ociLayouts {
		logger.InfoLn("Mirroring", repo)
		err = layouts.PushLayoutToRepoContext(
			ctx, ociLayout, repo,
			mirrorCtx.RegistryAuth,
			mirrorCtx.Logger,
			mirrorCtx.Parallelism,
			mirrorCtx.Insecure,
			mirrorCtx.SkipTLSVerification,
		)
		switch {
		case errors.Is(err, layouts.ErrEmptyLayout):
			logger.InfoF("Skipped repo %s as it contains no images", repo)
			continue
		case err != nil:
			return fmt.Errorf("Push Deckhouse to registry: %w", err)
		}

		logger.InfoF("Repo %s is mirrored", repo)
	}

	logger.InfoLn("All repositories are mirrored")

	if len(modulesList) == 0 {
		return nil
	}

	logger.InfoLn("Pushing modules tags")
	if err = pushModulesTags(ctx, &mirrorCtx.BaseContext, modulesList); err != nil {
		return fmt.Errorf("Push modules tags: %w", err)
	}
	logger.InfoF("All modules tags are pushed")

	return nil
}

func pushModulesTags(ctx context.Context, mirrorCtx *contexts.BaseContext, modulesList []string) error {
	if len(modulesList) == 0 {
		return nil
	}

	logger := mirrorCtx.Logger
	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorContext(mirrorCtx)
	remoteOpts = append(remoteOpts, remote.WithContext(ctx))
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
		pushCount++
	}
	return nil
}

func findLayoutsToPush(ctx context.Context, mirrorCtx *contexts.PushContext) (map[string]layout.Path, []string, error) {
	ociLayouts := make(map[string]layout.Path)
	bundlePaths := [][]string{
		{""}, // Root contains main deckhouse repo
		{"install"},
		{"install-standalone"},
		{"release-channel"},
		{"security", "trivy-db"},
		{"security", "trivy-bdu"},
		{"security", "trivy-java-db"},
		{"security", "trivy-checks"},
	}

	for _, bundlePath := range bundlePaths {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		indexRef := path.Join(append([]string{mirrorCtx.RegistryHost + mirrorCtx.RegistryPath}, bundlePath...)...)
		layoutFileSystemPath := filepath.Join(append([]string{mirrorCtx.UnpackedImagesPath}, bundlePath...)...)
		l, err := layout.FromPath(layoutFileSystemPath)
		if err != nil {
			return nil, nil, err
		}
		ociLayouts[indexRef] = l
	}

	modulesPath := filepath.Join(mirrorCtx.UnpackedImagesPath, "modules")
	modulesNames := make([]string, 0)
	dirs, err := os.ReadDir(modulesPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return ociLayouts, []string{}, nil
	case err != nil:
		return nil, nil, err
	}

	for _, dirEntry := range dirs {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}

		if !dirEntry.IsDir() {
			continue
		}

		moduleName := dirEntry.Name()
		modulesNames = append(modulesNames, moduleName)
		moduleRef := path.Join(mirrorCtx.RegistryHost+mirrorCtx.RegistryPath, "modules", moduleName)
		moduleReleasesRef := path.Join(mirrorCtx.RegistryHost+mirrorCtx.RegistryPath, "modules", moduleName, "release")
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
