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
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
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

var (
	TempDir = filepath.Join(os.TempDir(), "mirror")

	RegistryHost     string
	RegistryPath     string
	RegistryUsername string
	RegistryPassword string

	Insecure         bool
	TLSSkipVerify    bool
	ImagesBundlePath string
)

func push(_ *cobra.Command, _ []string) error {
	mirrorCtx := buildPushContext()
	logger := mirrorCtx.Logger

	if RegistryUsername != "" {
		mirrorCtx.RegistryAuth = authn.FromConfig(authn.AuthConfig{
			Username: RegistryUsername,
			Password: RegistryPassword,
		})
	}

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

	bundleStat, err := os.Stat(mirrorCtx.BundlePath)
	if err != nil {
		return err
	}

	if filepath.Ext(mirrorCtx.BundlePath) == ".tar" && bundleStat.Mode().IsRegular() {
		err := logger.Process("Unpacking Deckhouse bundle", func() error {
			return bundle.Unpack(&mirrorCtx.BaseContext)
		})
		if err != nil {
			return err
		}
		defer os.RemoveAll(mirrorCtx.UnpackedImagesPath)
	} else if bundleStat.IsDir() {
		logger.InfoLn("Using bundle at", mirrorCtx.BundlePath)
		mirrorCtx.UnpackedImagesPath = mirrorCtx.BundlePath
		if err := bundle.ValidateUnpackedBundle(mirrorCtx); err != nil {
			return fmt.Errorf("Invalid bundle: %w", err)
		}
	} else {
		return fmt.Errorf("bundle is not a tarball or directory")
	}

	err = logger.Process("Push Deckhouse images to registry", func() error {
		return operations.PushDeckhouseToRegistry(mirrorCtx)
	})
	if err != nil {
		return err
	}

	return nil
}

func buildPushContext() *contexts.PushContext {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	logger := log.NewSLogger(logLevel)

	mirrorCtx := &contexts.PushContext{
		BaseContext: contexts.BaseContext{
			Ctx:                 context.TODO(),
			Logger:              logger,
			Insecure:            Insecure,
			SkipTLSVerification: TLSSkipVerify,
			RegistryHost:        RegistryHost,
			RegistryPath:        RegistryPath,
			BundlePath:          ImagesBundlePath,
			UnpackedImagesPath:  filepath.Join(TempDir, time.Now().Format("mirror_tmp_02-01-2006_15-04-05")),
		},

		Parallelism: contexts.ParallelismConfig{
			Blobs:  4,
			Images: 1,
		},
	}
	return mirrorCtx
}
