package operations

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

func PullSecurityDatabases(pullParams *params.PullParams) error {
	var err error
	logger := pullParams.Logger
	tmpDir := filepath.Join(pullParams.WorkingDir, "security")

	imageLayouts := &layouts.ImageLayouts{}
	imageLayouts.TrivyDB, err = layouts.CreateEmptyImageLayout(filepath.Join(tmpDir, "trivy-db"))
	if err != nil {
		return fmt.Errorf("setup trivy db layout: %w", err)
	}
	imageLayouts.TrivyBDU, err = layouts.CreateEmptyImageLayout(filepath.Join(tmpDir, "trivy-bdu"))
	if err != nil {
		return fmt.Errorf("setup bdu layout: %w", err)
	}
	imageLayouts.TrivyJavaDB, err = layouts.CreateEmptyImageLayout(filepath.Join(tmpDir, "trivy-java-db"))
	if err != nil {
		return fmt.Errorf("setup java db layout: %w", err)
	}
	imageLayouts.TrivyChecks, err = layouts.CreateEmptyImageLayout(filepath.Join(tmpDir, "trivy-checks"))
	if err != nil {
		return fmt.Errorf("setup java db layout: %w", err)
	}

	if err := layouts.PullTrivyVulnerabilityDatabasesImages(pullParams, imageLayouts); err != nil {
		return fmt.Errorf("Pull Secutity Databases: %w", err)
	}

	logger.InfoLn("Processing image indexes")
	for _, l := range imageLayouts.Layouts() {
		err = layouts.SortIndexManifests(l)
		if err != nil {
			return fmt.Errorf("Sorting index manifests of %s: %w", l, err)
		}
	}

	if err = logger.Process("Pack security databases to security.tar", func() error {
		var securityDB io.Writer = chunked.NewChunkedFileWriter(
			pullParams.BundleChunkSize,
			pullParams.BundleDir,
			"security.tar",
		)
		if pullParams.BundleChunkSize == 0 {
			securityDB, err = os.Create(filepath.Join(pullParams.BundleDir, "security.tar"))
			if err != nil {
				return fmt.Errorf("create security.tar: %w", err)
			}
		}

		if err = bundle.Pack(context.Background(), tmpDir, securityDB); err != nil {
			return fmt.Errorf("pack security.tar: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
