// Copyright 2025 Flant JSC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packages

import (
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/yaml"
)

const (
	// DefinitionFile is the filename for package metadata
	DefinitionFile = "package.yaml"

	TypeModule      = "Module"
	TypeApplication = "Application"
)

// Definition represents common package metadata loaded from package.yaml.
type Definition struct {
	APIVersion string `yaml:"apiVersion" json:"apiVersion"`
	Type       string `yaml:"type" json:"type"`
	Name       string `yaml:"name" json:"name"`

	Descriptions Descriptions `yaml:"descriptions,omitempty" json:"descriptions,omitempty"`
	Requirements Requirements `yaml:"requirements,omitempty" json:"requirements,omitempty"`
}

// Requirements are the platform and module dependencies the package declares.
// It mirrors the contract deckhouse-controller enforces when the package is
// installed; the plugin only checks that it is well-formed, since no cluster
// exists at build time.
type Requirements struct {
	Kubernetes VersionConstraint   `yaml:"kubernetes,omitempty" json:"kubernetes,omitempty"`
	Deckhouse  VersionConstraint   `yaml:"deckhouse,omitempty" json:"deckhouse,omitempty"`
	Modules    ModulesRequirements `yaml:"modules,omitempty" json:"modules,omitempty"`
}

// VersionConstraint wraps a semver constraint expression. Empty means "no constraint".
type VersionConstraint struct {
	Constraint string `yaml:"constraint,omitempty" json:"constraint,omitempty"`
}

// ModulesRequirements groups module dependencies by how they gate the package:
//   - Mandatory: the module must be enabled;
//   - Conditional: enforced only when the module is enabled;
//   - AnyOf: at least one member of each group must be enabled;
//   - NoneOf: no member of any group may be enabled.
type ModulesRequirements struct {
	Mandatory   []ModuleDependency `yaml:"mandatory,omitempty" json:"mandatory,omitempty"`
	Conditional []ModuleDependency `yaml:"conditional,omitempty" json:"conditional,omitempty"`
	AnyOf       []ModuleGroup      `yaml:"anyOf,omitempty" json:"anyOf,omitempty"`
	NoneOf      []ModuleGroup      `yaml:"noneOf,omitempty" json:"noneOf,omitempty"`
}

// ModuleDependency is a named module dependency with an optional semver constraint.
type ModuleDependency struct {
	Name       string `yaml:"name" json:"name"`
	Constraint string `yaml:"constraint,omitempty" json:"constraint,omitempty"`
}

// ModuleGroup is a named group of module dependencies, shared by the AnyOf and
// NoneOf buckets. Name is required and identifies the group in diagnostics.
type ModuleGroup struct {
	Name        string             `yaml:"name" json:"name"`
	Description string             `yaml:"description,omitempty" json:"description,omitempty"`
	Modules     []ModuleDependency `yaml:"modules" json:"modules"`
}

// Descriptions holds localized description text for the package.
type Descriptions struct {
	Ru string `json:"ru,omitempty" yaml:"ru,omitempty"`
	En string `json:"en,omitempty" yaml:"en,omitempty"`
}

// LoadDefinitionByDir reads and parses the package definition file from the given directory.
func LoadDefinitionByDir(dir string) (Definition, error) {
	raw, err := os.ReadFile(filepath.Join(dir, DefinitionFile))
	if err != nil {
		return Definition{}, fmt.Errorf("failed to read definition file: %w", err)
	}

	def := new(Definition)
	if err = yaml.Unmarshal(raw, def); err != nil {
		return Definition{}, fmt.Errorf("failed to unmarshal definition file: %w", err)
	}

	return *def, nil
}
