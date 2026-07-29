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

package internal

// Plugin represents a plugin domain entity
type Plugin struct {
	Name         string
	Version      string
	Description  string
	Env          []EnvVar
	Flags        []Flag
	Requirements Requirements
}

// EnvVar represents an environment variable required by the plugin
type EnvVar struct {
	Name string
}

// Flag represents a command-line flag supported by the plugin
type Flag struct {
	Name string
}

// Requirements represents plugin dependencies (v2 schema).
//
// v1 schema: flat array of plugin and module requirements
// v2 schema: structured groups (with matchers anyOf, mandatory, conditional) + deckhouse
type Requirements struct {
	Kubernetes KubernetesRequirement
	Deckhouse  DeckhouseRequirement
	Modules    ModuleRequirementsGroup
	Plugins    PluginRequirementsGroup
}

// KubernetesRequirement represents Kubernetes version constraint
type KubernetesRequirement struct {
	Constraint string
}

// DeckhouseRequirement represents a constraint on the Deckhouse cluster version.
type DeckhouseRequirement struct {
	Constraint string
}

// ModuleRequirement represents a required Deckhouse module
type ModuleRequirement struct {
	Name       string
	Constraint string
}

// PluginRequirement represents a required plugin
type PluginRequirement struct {
	Name       string
	Constraint string
}

// ModuleGroup is a named group of module requirements, shared by the AnyOf and
// NoneOf buckets of ModuleRequirementsGroup (bucket semantics are documented
// there). Name is a required, stable identifier used in diagnostics; Description
// is optional human-facing text.
type ModuleGroup struct {
	Name        string
	Description string
	Modules     []ModuleRequirement
}

// PluginRequirementsGroup splits plugin requirements into Mandatory and Conditional.
//   - Mandatory: the dependent plugin must be installed AND satisfy the constraint.
//   - Conditional: only enforced if the dependent plugin is installed; otherwise skipped.
type PluginRequirementsGroup struct {
	Mandatory   []PluginRequirement
	Conditional []PluginRequirement
}

// ModuleRequirementsGroup splits module requirements into Mandatory, Conditional,
// AnyOf, and NoneOf.
//   - Mandatory: the module must be enabled AND satisfy the constraint.
//   - Conditional: only enforced if the module is enabled.
//   - AnyOf: at least one module per group must be enabled and satisfy its constraint.
//   - NoneOf: no module in any group may be enabled (a member constraint, if set,
//     narrows the forbidden version range).
type ModuleRequirementsGroup struct {
	Mandatory   []ModuleRequirement
	Conditional []ModuleRequirement
	AnyOf       []ModuleGroup
	NoneOf      []ModuleGroup
}
