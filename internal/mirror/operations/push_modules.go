package operations

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

func PushModule(pushParams *params.PushParams, moduleName string, pkg io.Reader) error {
	packageDir := filepath.Join(pushParams.WorkingDir, "modules", moduleName)
	if err := os.MkdirAll(packageDir, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	defer os.RemoveAll(packageDir)

	if err := bundle.Unpack(context.Background(), pkg, packageDir); err != nil {
		return fmt.Errorf("Unpack package: %w", err)
	}

	if err := bundle.ValidateUnpackedPackage(bundle.MandatoryLayoutsForModule(packageDir)); err != nil {
		return fmt.Errorf("Invalid module package: %w", err)
	}

	// These are layouts within module-ABC.tar mapped to paths they belong to in the deckhouse registry.
	// Registry paths are relative to root of deckhouse repo.
	layoutsToPush := map[string]string{
		"":        path.Join("modules", moduleName),
		"release": path.Join("modules", moduleName, "release"),
	}

	for layoutPathSuffix, repo := range layoutsToPush {
		repoRef := path.Join(pushParams.RegistryHost, pushParams.RegistryPath, repo)
		pushParams.Logger.InfoLn("Pushing", repoRef)
		if err := layouts.PushLayoutToRepoContext(
			context.Background(),
			layout.Path(filepath.Join(packageDir, layoutPathSuffix)),
			repoRef,
			pushParams.RegistryAuth,
			pushParams.Logger,
			pushParams.Parallelism,
			pushParams.Insecure,
			pushParams.SkipTLSVerification,
		); err != nil {
			return fmt.Errorf("Push module package: %w", err)
		}
	}

	pushParams.Logger.InfoF("Pushing module tag for %s", moduleName)
	refOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptionsFromMirrorParams(&pushParams.BaseParams)
	modulesRepo := path.Join(pushParams.RegistryHost, pushParams.RegistryPath, "modules")
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

	return nil
}
