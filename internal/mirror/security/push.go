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

package security

import (
	"context"
	"path/filepath"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/pusher"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

// PushOptions contains options for pushing security images
type PushOptions struct {
	BundleDir  string
	WorkingDir string
}

// PushService handles pushing security database images to registry
type PushService struct {
	client        registry.Client
	pusherService *pusher.Service
	options       *PushOptions
}

// NewPushService creates a new security push service
func NewPushService(
	client registry.Client,
	options *PushOptions,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PushService {
	if options == nil {
		options = &PushOptions{}
	}

	return &PushService{
		client:        client,
		pusherService: pusher.NewService(logger, userLogger),
		options:       options,
	}
}

// Push pushes the security package to the registry
func (svc *PushService) Push(ctx context.Context) error {
	return svc.pusherService.PushPackage(ctx, pusher.PackagePushConfig{
		PackageName: "security",
		ProcessName: "Push security databases",
		WorkingDir:  svc.options.WorkingDir,
		BundleDir:   svc.options.BundleDir,
		Client:      svc.client.WithSegment("security"),
		// New pull creates layouts at security/trivy-db, security/trivy-bdu, etc.
		MandatoryLayoutsFunc: func(packageDir string) map[string]string {
			return map[string]string{
				"trivy database layout":      filepath.Join(packageDir, "security", "trivy-db"),
				"trivy bdu layout":           filepath.Join(packageDir, "security", "trivy-bdu"),
				"trivy java database layout": filepath.Join(packageDir, "security", "trivy-java-db"),
				"trivy checks layout":        filepath.Join(packageDir, "security", "trivy-checks"),
			}
		},
		Layouts: []pusher.LayoutMapping{
			{LayoutPath: "security/trivy-db", Segment: "trivy-db"},
			{LayoutPath: "security/trivy-java-db", Segment: "trivy-java-db"},
			{LayoutPath: "security/trivy-bdu", Segment: "trivy-bdu"},
			{LayoutPath: "security/trivy-checks", Segment: "trivy-checks"},
		},
	})
}
