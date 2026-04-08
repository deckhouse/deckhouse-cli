// Copyright 2026 Flant JSC
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	"github.com/deckhouse/deckhouse-cli/pkg/stub"
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

func TestService_validateSecurityAccess(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	t.Run("trivy-db tag 2 exists – no error", func(t *testing.T) {
		reg := stub.NewRegistry("registry.example.com")
		trivyImg := stub.NewImageBuilder().MustBuild()
		reg.MustAddImage("security/trivy-db", "2", trivyImg)

		stubClient := stub.NewClient(reg)
		securityClient := stubClient.WithSegment("security")
		securityService := registryservice.NewSecurityServices("security", securityClient, logger)

		svc := newTestSecurityService(securityService, logger, userLogger)
		err := svc.validateSecurityAccess(context.Background())
		require.NoError(t, err)
	})

	t.Run("trivy-db tag 2 absent – no error (graceful skip)", func(t *testing.T) {
		reg := stub.NewRegistry("registry.example.com")
		stubClient := stub.NewClient(reg)
		securityClient := stubClient.WithSegment("security")
		securityService := registryservice.NewSecurityServices("security", securityClient, logger)

		svc := newTestSecurityService(securityService, logger, userLogger)
		err := svc.validateSecurityAccess(context.Background())
		require.NoError(t, err)
	})
}

// TestService_validateSecurityAccess_MultipleDatabases verifies that when all
// security databases are present the service reports no error.
func TestService_validateSecurityAccess_MultipleDatabases(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	reg := stub.NewRegistry("registry.example.com")
	trivyImg := stub.NewImageBuilder().MustBuild()

	for _, dbSegment := range []string{
		internal.SecurityTrivyDBSegment,
		internal.SecurityTrivyBDUSegment,
		internal.SecurityTrivyJavaDBSegment,
		internal.SecurityTrivyChecksSegment,
	} {
		reg.MustAddImage("security/"+dbSegment, "2", trivyImg)
	}

	stubClient := stub.NewClient(reg)
	securityClient := stubClient.WithSegment("security")
	securityService := registryservice.NewSecurityServices("security", securityClient, logger)

	svc := newTestSecurityService(securityService, logger, userLogger)
	err := svc.validateSecurityAccess(context.Background())
	require.NoError(t, err)
}

// TestService_validateSecurityAccess_PerDatabase exercises validateSecurityAccess
// for each known security database variant. Only trivy-db is checked during
// access validation; the others are handled with AllowMissingTags in the puller.
func TestService_validateSecurityAccess_PerDatabase(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	databases := []struct {
		segment string
		wantErr bool
	}{
		{internal.SecurityTrivyDBSegment, false},
		{internal.SecurityTrivyBDUSegment, false},
		{internal.SecurityTrivyJavaDBSegment, false},
		{internal.SecurityTrivyChecksSegment, false},
	}

	for _, db := range databases {
		t.Run("database "+db.segment+" with tag 2 present", func(t *testing.T) {
			reg := stub.NewRegistry("registry.example.com")
			dbImg := stub.NewImageBuilder().MustBuild()
			reg.MustAddImage("security/"+db.segment, "2", dbImg)

			stubClient := stub.NewClient(reg)
			securityClient := stubClient.WithSegment("security")
			securityService := registryservice.NewSecurityServices("security", securityClient, logger)

			svc := newTestSecurityService(securityService, logger, userLogger)
			err := svc.validateSecurityAccess(context.Background())

			if db.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
