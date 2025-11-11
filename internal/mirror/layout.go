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

package mirror

import (
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/platform"
)

type ModuleRelease struct {
	Version string
}

type ModuleImageLayout struct {
	ModuleLayout layout.Path
	ModuleImages map[string]struct{}

	ReleasesLayout layout.Path
	ReleaseImages  map[string]ModuleRelease

	ExtraLayout layout.Path
	ExtraImages map[string]struct{}
}

type ImageLayouts struct {
	platform v1.Platform

	DeckhousePlatform *platform.ImageLayouts

	TrivyDB           layout.Path
	TrivyDBImages     map[string]struct{}
	TrivyBDU          layout.Path
	TrivyBDUImages    map[string]struct{}
	TrivyJavaDB       layout.Path
	TrivyJavaDBImages map[string]struct{}
	TrivyChecks       layout.Path
	TrivyChecksImages map[string]struct{}

	Modules map[string]ModuleImageLayout
}

func NewImageLayouts() *ImageLayouts {
	return &ImageLayouts{
		platform: v1.Platform{Architecture: "amd64", OS: "linux"},
		Modules:  make(map[string]ModuleImageLayout),
	}
}
