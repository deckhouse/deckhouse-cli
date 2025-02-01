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

package operations

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

func PushDeckhousePlatform(pushParams *params.PushParams, pkg io.Reader) error {
	packageDir := filepath.Join(pushParams.WorkingDir, "platform")
	if err := os.MkdirAll(packageDir, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	defer os.RemoveAll(packageDir)

	pushParams.Logger.InfoLn("Unpacking platform package")
	if err := bundle.Unpack(context.Background(), pkg, packageDir); err != nil {
		return fmt.Errorf("Unpack package: %w", err)
	}

	pushParams.Logger.InfoLn("Validating platform package")
	if err := bundle.ValidateUnpackedPackage(bundle.MandatoryLayoutsForPlatform(packageDir)); err != nil {
		return fmt.Errorf("Invalid platform package: %w", err)
	}

	// These are layouts within platform.tar
	layoutsToPush := []string{
		"",                   // Root layout
		"install",            // Installer images
		"install-standalone", // Standalone installer bundles
		"release-channel",    // Release channels
	}

	for _, repo := range layoutsToPush {
		repoRef := path.Join(pushParams.RegistryHost, pushParams.RegistryPath, repo)
		pushParams.Logger.InfoLn("Pushing", repoRef)
		if err := layouts.PushLayoutToRepoContext(
			context.Background(),
			layout.Path(filepath.Join(packageDir, repo)),
			repoRef,
			pushParams.RegistryAuth,
			pushParams.Logger,
			pushParams.Parallelism,
			pushParams.Insecure,
			pushParams.SkipTLSVerification,
		); err != nil {
			return fmt.Errorf("Push platform package: %w", err)
		}
	}

	return nil
}
