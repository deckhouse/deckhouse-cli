// Copyright 2026 Flant JSC
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

func newTestSecurityService(
	securityService *registryservice.SecurityServices,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	return &Service{
		securityService: securityService,
		options:         &Options{},
		logger:          logger,
		userLogger:      userLogger,
	}
}

func TestService_securityDatabasesAvailable(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	t.Run("trivy-db tag 2 exists – available", func(t *testing.T) {
		reg := upfake.NewRegistry("registry.example.com")
		trivyImg := upfake.NewImageBuilder().MustBuild()
		reg.MustAddImage("security/trivy-db", "2", trivyImg)

		stubClient := pkgclient.Adapt(upfake.NewClient(reg))
		securityClient := stubClient.WithSegment("security")
		securityService := registryservice.NewSecurityServices("security", securityClient, logger)

		svc := newTestSecurityService(securityService, logger, userLogger)
		available, err := svc.securityDatabasesAvailable(context.Background())
		require.NoError(t, err)
		require.True(t, available)
	})

	t.Run("trivy-db tag 2 absent – not available (graceful skip)", func(t *testing.T) {
		reg := upfake.NewRegistry("registry.example.com")
		stubClient := pkgclient.Adapt(upfake.NewClient(reg))
		securityClient := stubClient.WithSegment("security")
		securityService := registryservice.NewSecurityServices("security", securityClient, logger)

		svc := newTestSecurityService(securityService, logger, userLogger)
		available, err := svc.securityDatabasesAvailable(context.Background())
		require.NoError(t, err)
		require.False(t, available)
	})
}

// TestService_securityDatabasesAvailable_MultipleDatabases verifies that when all
// security databases are present the service reports available.
func TestService_securityDatabasesAvailable_MultipleDatabases(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	reg := upfake.NewRegistry("registry.example.com")
	trivyImg := upfake.NewImageBuilder().MustBuild()

	for _, dbSegment := range []string{
		internal.SecurityTrivyDBSegment,
		internal.SecurityTrivyBDUSegment,
		internal.SecurityTrivyJavaDBSegment,
		internal.SecurityTrivyChecksSegment,
	} {
		reg.MustAddImage("security/"+dbSegment, "2", trivyImg)
	}

	stubClient := pkgclient.Adapt(upfake.NewClient(reg))
	securityClient := stubClient.WithSegment("security")
	securityService := registryservice.NewSecurityServices("security", securityClient, logger)

	svc := newTestSecurityService(securityService, logger, userLogger)
	available, err := svc.securityDatabasesAvailable(context.Background())
	require.NoError(t, err)
	require.True(t, available)
}

// TestService_securityDatabasesAvailable_PerDatabase exercises securityDatabasesAvailable
// for each known security database variant. Only trivy-db:2 is checked during
// the availability check; the others are handled with AllowMissingTags in the puller.
func TestService_securityDatabasesAvailable_PerDatabase(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	databases := []struct {
		segment       string
		wantAvailable bool
	}{
		{internal.SecurityTrivyDBSegment, true},      // trivy-db:2 exists - available
		{internal.SecurityTrivyBDUSegment, false},    // only trivy-bdu:2 added, but check looks for trivy-db:2
		{internal.SecurityTrivyJavaDBSegment, false}, // same - trivy-db:2 not present
		{internal.SecurityTrivyChecksSegment, false}, // same - trivy-db:2 not present
	}

	for _, db := range databases {
		t.Run("database "+db.segment+" with tag 2 present", func(t *testing.T) {
			reg := upfake.NewRegistry("registry.example.com")
			dbImg := upfake.NewImageBuilder().MustBuild()
			reg.MustAddImage("security/"+db.segment, "2", dbImg)

			stubClient := pkgclient.Adapt(upfake.NewClient(reg))
			securityClient := stubClient.WithSegment("security")
			securityService := registryservice.NewSecurityServices("security", securityClient, logger)

			svc := newTestSecurityService(securityService, logger, userLogger)
			available, err := svc.securityDatabasesAvailable(context.Background())
			require.NoError(t, err)
			require.Equal(t, db.wantAvailable, available)
		})
	}
}

// TestPullSecurity_SkipsWhenDatabasesNotAvailable verifies the full PullSecurity flow
// (not dry-run) when security databases don't exist in the registry.
// This simulates CE/BE/SE editions where security images are not published.
// Expected: PullSecurity returns nil (no error), and security.tar is NOT created.
func TestPullSecurity_SkipsWhenDatabasesNotAvailable(t *testing.T) {
	workingDir := t.TempDir()
	// bundleDir  - where security.tar would be written if pull proceeded (but it shouldn't in this test)
	bundleDir := t.TempDir()

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	// Empty registry - no security images (trivy-db, trivy-bdu, etc.)
	reg := upfake.NewRegistry("registry.example.com")
	stubClient := pkgclient.Adapt(upfake.NewClient(reg))

	regSvc := registryservice.NewService(stubClient, pkg.FEEdition, logger)
	securitySvc := NewService(regSvc, workingDir, &Options{BundleDir: bundleDir}, logger, userLogger)

	err := securitySvc.PullSecurity(context.Background())
	require.NoError(t, err)

	// bundleDir must be empty when databases are not available - security.tar should not be created.
	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	require.Empty(t, entries, "security.tar should not be created when databases are not available")
}
