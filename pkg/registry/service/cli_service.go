/*
Copyright 2026 Flant JSC

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

package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/deckhouse/deckhouse/pkg/log"
	client "github.com/deckhouse/deckhouse/pkg/registry"
)

// The d8 binary itself is published as a multi-platform OCI index under
// <root>/deckhouse-cli - the bare registry root, outside the edition segment,
// next to the plugins catalog it owns:
//
//	<root>/deckhouse-cli:<vX.Y.Z>           - one CLI release
//	<root>/deckhouse-cli/plugins            - plugin catalog (PluginsService)
//
// Tags are plain versions; the registry-packages-proxy serves this repository
// to `d8 dist update`, which is why an air-gapped bundle has to carry it.
const cliServiceName = "deckhouse-cli"

// CLIService is scoped to the deckhouse-cli repository. ListTags on it
// enumerates the published CLI versions.
type CLIService struct {
	client client.Client

	*BasicService

	logger *log.Logger
}

// NewCLIService creates a service for the deckhouse-cli repository.
func NewCLIService(client client.Client, logger *log.Logger) *CLIService {
	return &CLIService{
		client: client,

		BasicService: NewBasicService(cliServiceName, client, logger),

		logger: logger,
	}
}

// GetRoot returns the full registry path of the deckhouse-cli repository.
func (s *CLIService) GetRoot() string {
	return s.client.GetRegistry()
}

// GetManifest returns the raw manifest structure for tag. A release resolves
// to an index whose children carry the per-platform binaries.
func (s *CLIService) GetManifest(ctx context.Context, tag string) (client.ManifestResult, error) {
	logger := s.logger.With(slog.String("service", cliServiceName), slog.String("tag", tag))

	logger.Debug("Getting manifest")

	result, err := s.client.GetManifest(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	logger.Debug("Manifest retrieved successfully")

	return result, nil
}
