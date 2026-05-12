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

package service

import (
	"encoding/json"
	"fmt"
)

// PluginContract represents the plugin contract metadata (DTO for JSON unmarshaling)
type PluginContract struct {
	Name         string          `json:"name"`
	Version      string          `json:"version"`
	Description  string          `json:"description"`
	Env          []EnvVarDTO     `json:"env,omitempty"`
	Flags        []FlagDTO       `json:"flags,omitempty"`
	Requirements RequirementsDTO `json:"requirements,omitempty"`
}

// EnvVarDTO represents an environment variable in JSON
type EnvVarDTO struct {
	Name string `json:"name"`
}

// FlagDTO represents a flag in JSON
type FlagDTO struct {
	Name string `json:"name"`
}

// RequirementsDTO represents requirements in JSON.
type RequirementsDTO struct {
	Kubernetes KubernetesRequirementDTO   `json:"kubernetes,omitempty"`
	Deckhouse  DeckhouseRequirementDTO    `json:"deckhouse,omitempty"`
	Modules    ModuleRequirementsGroupDTO `json:"modules,omitempty"`
	Plugins    PluginRequirementsGroupDTO `json:"plugins,omitempty"`
}

// KubernetesRequirementDTO represents Kubernetes requirement in JSON
type KubernetesRequirementDTO struct {
	Constraint string `json:"constraint"`
}

// DeckhouseRequirementDTO represents a constraint on the running Deckhouse version.
type DeckhouseRequirementDTO struct {
	Constraint string `json:"constraint"`
}

// ModuleRequirementDTO represents module requirement in JSON
type ModuleRequirementDTO struct {
	Name       string `json:"name"`
	Constraint string `json:"constraint"`
}

// PluginRequirementDTO represents plugin requirement in JSON
type PluginRequirementDTO struct {
	Name       string `json:"name"`
	Constraint string `json:"constraint"`
}

// AnyOfGroupDTO represents an "at least one of" group of module requirements.
// The Description is surfaced in user-facing error messages when no module in
// the group satisfies the constraint.
type AnyOfGroupDTO struct {
	Description string                 `json:"description,omitempty"`
	Modules     []ModuleRequirementDTO `json:"modules,omitempty"`
}

// PluginRequirementsGroupDTO splits plugin requirements into Mandatory and
// Conditional sections. v1 flat arrays unmarshal into .Mandatory.
type PluginRequirementsGroupDTO struct {
	Mandatory   []PluginRequirementDTO `json:"mandatory,omitempty"`
	Conditional []PluginRequirementDTO `json:"conditional,omitempty"`
}

// UnmarshalJSON accepts both v2 object form and v1 flat-array form.
// v1 array contents are placed in .Mandatory.
func (g *PluginRequirementsGroupDTO) UnmarshalJSON(data []byte) error {
	type alias PluginRequirementsGroupDTO
	var asGroup alias
	if err := json.Unmarshal(data, &asGroup); err == nil {
		*g = PluginRequirementsGroupDTO(asGroup)
		return nil
	}
	var asArray []PluginRequirementDTO
	if err := json.Unmarshal(data, &asArray); err == nil {
		g.Mandatory = asArray
		g.Conditional = nil
		return nil
	}
	return fmt.Errorf("plugins requirements: expected array (v1) or object (v2)")
}

// ModuleRequirementsGroupDTO splits module requirements into Mandatory,
// Conditional, and AnyOf sections. v1 flat arrays unmarshal into .Mandatory.
type ModuleRequirementsGroupDTO struct {
	Mandatory   []ModuleRequirementDTO `json:"mandatory,omitempty"`
	Conditional []ModuleRequirementDTO `json:"conditional,omitempty"`
	AnyOf       []AnyOfGroupDTO        `json:"anyOf,omitempty"`
}

// UnmarshalJSON accepts both v2 object form and v1 flat-array form.
// v1 array contents are placed in .Mandatory.
func (g *ModuleRequirementsGroupDTO) UnmarshalJSON(data []byte) error {
	type alias ModuleRequirementsGroupDTO
	var asGroup alias
	if err := json.Unmarshal(data, &asGroup); err == nil {
		*g = ModuleRequirementsGroupDTO(asGroup)
		return nil
	}
	var asArray []ModuleRequirementDTO
	if err := json.Unmarshal(data, &asArray); err == nil {
		g.Mandatory = asArray
		g.Conditional = nil
		g.AnyOf = nil
		return nil
	}
	return fmt.Errorf("modules requirements: expected array (v1) or object (v2)")
}
