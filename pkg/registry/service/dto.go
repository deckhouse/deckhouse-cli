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
// Conditional sections.
type PluginRequirementsGroupDTO struct {
	Mandatory   []PluginRequirementDTO `json:"mandatory,omitempty"`
	Conditional []PluginRequirementDTO `json:"conditional,omitempty"`
}

// ModuleRequirementsGroupDTO splits module requirements into Mandatory,
// Conditional, and AnyOf sections.
type ModuleRequirementsGroupDTO struct {
	Mandatory   []ModuleRequirementDTO `json:"mandatory,omitempty"`
	Conditional []ModuleRequirementDTO `json:"conditional,omitempty"`
	AnyOf       []AnyOfGroupDTO        `json:"anyOf,omitempty"`
}
