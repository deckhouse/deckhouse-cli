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

package platform

import (
	"context"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/pusher"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

// PushOptions contains options for pushing platform images
type PushOptions struct {
	BundleDir  string
	WorkingDir string
}

// PushService handles pushing platform images to registry
type PushService struct {
	client        registry.Client
	pusherService *pusher.Service
	options       *PushOptions
}

// NewPushService creates a new platform push service
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

// Push pushes the platform package to the registry
func (svc *PushService) Push(ctx context.Context) error {
	return svc.pusherService.PushPackage(ctx, pusher.PackagePushConfig{
		PackageName:          "platform",
		ProcessName:          "Push Deckhouse platform",
		WorkingDir:           svc.options.WorkingDir,
		BundleDir:            svc.options.BundleDir,
		Client:               svc.client,
		MandatoryLayoutsFunc: bundle.MandatoryLayoutsForPlatform,
		Layouts: []pusher.LayoutMapping{
			{LayoutPath: "", Segment: ""},                                     // Root layout
			{LayoutPath: "install", Segment: "install"},                       // Installer images
			{LayoutPath: "install-standalone", Segment: "install-standalone"}, // Standalone installer
			{LayoutPath: "release-channel", Segment: "release-channel"},       // Release channels
		},
	})
}
