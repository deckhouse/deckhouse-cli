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
	"fmt"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// securityImageLayout wraps image.ImageLayout for security databases
type securityImageLayout struct {
	imageLayout *image.ImageLayout
}

// SecurityLayouts manages OCI image layouts for security databases
type SecurityLayouts struct {
	workingDir string

	trivyDB      *securityImageLayout
	trivyBDU     *securityImageLayout
	trivyJavaDB  *securityImageLayout
	trivyChecks  *securityImageLayout
}

// NewSecurityLayouts creates new security layouts in the specified directory
func NewSecurityLayouts(workingDir string) (*SecurityLayouts, error) {
	securityDir := filepath.Join(workingDir, "security")

	trivyDB, err := image.NewImageLayout(filepath.Join(securityDir, internal.SecurityTrivyDBSegment))
	if err != nil {
		return nil, fmt.Errorf("create trivy-db layout: %w", err)
	}

	trivyBDU, err := image.NewImageLayout(filepath.Join(securityDir, internal.SecurityTrivyBDUSegment))
	if err != nil {
		return nil, fmt.Errorf("create trivy-bdu layout: %w", err)
	}

	trivyJavaDB, err := image.NewImageLayout(filepath.Join(securityDir, internal.SecurityTrivyJavaDBSegment))
	if err != nil {
		return nil, fmt.Errorf("create trivy-java-db layout: %w", err)
	}

	trivyChecks, err := image.NewImageLayout(filepath.Join(securityDir, internal.SecurityTrivyChecksSegment))
	if err != nil {
		return nil, fmt.Errorf("create trivy-checks layout: %w", err)
	}

	return &SecurityLayouts{
		workingDir:  securityDir,
		trivyDB:     &securityImageLayout{imageLayout: trivyDB},
		trivyBDU:    &securityImageLayout{imageLayout: trivyBDU},
		trivyJavaDB: &securityImageLayout{imageLayout: trivyJavaDB},
		trivyChecks: &securityImageLayout{imageLayout: trivyChecks},
	}, nil
}

func (l *SecurityLayouts) WorkingDir() string {
	return l.workingDir
}

func (l *SecurityLayouts) TrivyDB() *securityImageLayout {
	return l.trivyDB
}

func (l *SecurityLayouts) TrivyBDU() *securityImageLayout {
	return l.trivyBDU
}

func (l *SecurityLayouts) TrivyJavaDB() *securityImageLayout {
	return l.trivyJavaDB
}

func (l *SecurityLayouts) TrivyChecks() *securityImageLayout {
	return l.trivyChecks
}

func (l *SecurityLayouts) AsList() []*image.ImageLayout {
	return []*image.ImageLayout{
		l.trivyDB.imageLayout,
		l.trivyBDU.imageLayout,
		l.trivyJavaDB.imageLayout,
		l.trivyChecks.imageLayout,
	}
}

