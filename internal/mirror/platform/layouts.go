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
	"fmt"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// Layouts manages OCI image layouts for platform components
type Layouts struct {
	workingDir string

	deckhouse           *image.ImageLayout
	releaseChannels     *image.ImageLayout
	installer           *image.ImageLayout
	standaloneInstaller *image.ImageLayout
}

// NewLayouts creates new platform layouts in the specified directory
func NewLayouts(workingDir string) (*Layouts, error) {
	platformDir := filepath.Join(workingDir, "platform")

	deckhouse, err := image.NewImageLayout(filepath.Join(platformDir, "deckhouse"))
	if err != nil {
		return nil, fmt.Errorf("create deckhouse layout: %w", err)
	}

	releaseChannels, err := image.NewImageLayout(filepath.Join(platformDir, "release-channel"))
	if err != nil {
		return nil, fmt.Errorf("create release-channel layout: %w", err)
	}

	installer, err := image.NewImageLayout(filepath.Join(platformDir, "install"))
	if err != nil {
		return nil, fmt.Errorf("create install layout: %w", err)
	}

	standaloneInstaller, err := image.NewImageLayout(filepath.Join(platformDir, "install-standalone"))
	if err != nil {
		return nil, fmt.Errorf("create install-standalone layout: %w", err)
	}

	return &Layouts{
		workingDir:          platformDir,
		deckhouse:           deckhouse,
		releaseChannels:     releaseChannels,
		installer:           installer,
		standaloneInstaller: standaloneInstaller,
	}, nil
}

func (l *Layouts) WorkingDir() string {
	return l.workingDir
}

func (l *Layouts) Deckhouse() *image.ImageLayout {
	return l.deckhouse
}

func (l *Layouts) ReleaseChannels() *image.ImageLayout {
	return l.releaseChannels
}

func (l *Layouts) Installer() *image.ImageLayout {
	return l.installer
}

func (l *Layouts) StandaloneInstaller() *image.ImageLayout {
	return l.standaloneInstaller
}

func (l *Layouts) AsList() []*image.ImageLayout {
	return []*image.ImageLayout{
		l.deckhouse,
		l.releaseChannels,
		l.installer,
		l.standaloneInstaller,
	}
}
