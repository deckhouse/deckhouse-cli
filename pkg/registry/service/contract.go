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
	"errors"
	"fmt"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// UnmarshalContract decodes raw contract JSON into dst.
// It rewrites encoding/json's default errors as user-actionable messages.
// Wording stays identical across sources: a cache file or a
// registry-packages-proxy tar entry.
func UnmarshalContract(raw []byte, dst *PluginContract) error {
	err := json.Unmarshal(raw, dst)
	if err == nil {
		return nil
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		if (typeErr.Field == "requirements.modules" || typeErr.Field == "requirements.plugins") && typeErr.Value == "array" {
			return fmt.Errorf("invalid contract: field %q must be an object with mandatory/conditional sections, got a JSON array", typeErr.Field)
		}

		if typeErr.Field != "" {
			return fmt.Errorf("invalid contract: field %q has wrong JSON type (got %s)", typeErr.Field, typeErr.Value)
		}

		return fmt.Errorf("invalid contract: wrong JSON type (got %s)", typeErr.Value)
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return fmt.Errorf("invalid contract: malformed JSON at byte offset %d", syntaxErr.Offset)
	}

	return fmt.Errorf("invalid contract: %w", err)
}

// ContractToDomain converts PluginContract DTO to Plugin domain entity.
func ContractToDomain(contract *PluginContract) *internal.Plugin {
	plugin := &internal.Plugin{
		Name:        contract.Name,
		Version:     contract.Version,
		Description: contract.Description,
		Env:         make([]internal.EnvVar, 0, len(contract.Env)),
		Flags:       make([]internal.Flag, 0, len(contract.Flags)),
	}

	for _, envDTO := range contract.Env {
		plugin.Env = append(plugin.Env, internal.EnvVar{Name: envDTO.Name})
	}

	for _, flagDTO := range contract.Flags {
		plugin.Flags = append(plugin.Flags, internal.Flag{Name: flagDTO.Name})
	}

	plugin.Requirements = internal.Requirements{
		Kubernetes: internal.KubernetesRequirement{Constraint: contract.Requirements.Kubernetes.Constraint},
		Deckhouse:  internal.DeckhouseRequirement{Constraint: contract.Requirements.Deckhouse.Constraint},
		Modules:    moduleGroupToDomain(contract.Requirements.Modules),
		Plugins:    pluginGroupToDomain(contract.Requirements.Plugins),
	}

	return plugin
}

// DomainToContract converts Plugin domain entity to PluginContract DTO.
func DomainToContract(plugin *internal.Plugin) *PluginContract {
	contract := &PluginContract{
		Name:        plugin.Name,
		Version:     plugin.Version,
		Description: plugin.Description,
		Env:         make([]EnvVarDTO, 0, len(plugin.Env)),
		Flags:       make([]FlagDTO, 0, len(plugin.Flags)),
		Requirements: RequirementsDTO{
			Kubernetes: KubernetesRequirementDTO{Constraint: plugin.Requirements.Kubernetes.Constraint},
			Deckhouse:  DeckhouseRequirementDTO{Constraint: plugin.Requirements.Deckhouse.Constraint},
			Modules:    moduleGroupToDTO(plugin.Requirements.Modules),
			Plugins:    pluginGroupToDTO(plugin.Requirements.Plugins),
		},
	}

	for _, env := range plugin.Env {
		contract.Env = append(contract.Env, EnvVarDTO{Name: env.Name})
	}

	for _, flag := range plugin.Flags {
		contract.Flags = append(contract.Flags, FlagDTO{Name: flag.Name})
	}

	return contract
}

func pluginGroupToDomain(g PluginRequirementsGroupDTO) internal.PluginRequirementsGroup {
	return internal.PluginRequirementsGroup{
		Mandatory:   pluginReqsToDomain(g.Mandatory),
		Conditional: pluginReqsToDomain(g.Conditional),
	}
}

func pluginGroupToDTO(g internal.PluginRequirementsGroup) PluginRequirementsGroupDTO {
	return PluginRequirementsGroupDTO{
		Mandatory:   pluginReqsToDTO(g.Mandatory),
		Conditional: pluginReqsToDTO(g.Conditional),
	}
}

func pluginReqsToDomain(reqs []PluginRequirementDTO) []internal.PluginRequirement {
	out := make([]internal.PluginRequirement, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, internal.PluginRequirement{Name: r.Name, Constraint: r.Constraint})
	}

	return out
}

func pluginReqsToDTO(reqs []internal.PluginRequirement) []PluginRequirementDTO {
	out := make([]PluginRequirementDTO, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, PluginRequirementDTO{Name: r.Name, Constraint: r.Constraint})
	}

	return out
}

func moduleGroupToDomain(g ModuleRequirementsGroupDTO) internal.ModuleRequirementsGroup {
	anyOf := make([]internal.AnyOfGroup, 0, len(g.AnyOf))
	for _, grp := range g.AnyOf {
		anyOf = append(anyOf, internal.AnyOfGroup{
			Description: grp.Description,
			Modules:     moduleReqsToDomain(grp.Modules),
		})
	}

	return internal.ModuleRequirementsGroup{
		Mandatory:   moduleReqsToDomain(g.Mandatory),
		Conditional: moduleReqsToDomain(g.Conditional),
		AnyOf:       anyOf,
	}
}

func moduleGroupToDTO(g internal.ModuleRequirementsGroup) ModuleRequirementsGroupDTO {
	anyOf := make([]AnyOfGroupDTO, 0, len(g.AnyOf))
	for _, grp := range g.AnyOf {
		anyOf = append(anyOf, AnyOfGroupDTO{
			Description: grp.Description,
			Modules:     moduleReqsToDTO(grp.Modules),
		})
	}

	return ModuleRequirementsGroupDTO{
		Mandatory:   moduleReqsToDTO(g.Mandatory),
		Conditional: moduleReqsToDTO(g.Conditional),
		AnyOf:       anyOf,
	}
}

func moduleReqsToDomain(reqs []ModuleRequirementDTO) []internal.ModuleRequirement {
	out := make([]internal.ModuleRequirement, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, internal.ModuleRequirement{Name: r.Name, Constraint: r.Constraint})
	}

	return out
}

func moduleReqsToDTO(reqs []internal.ModuleRequirement) []ModuleRequirementDTO {
	out := make([]ModuleRequirementDTO, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, ModuleRequirementDTO{Name: r.Name, Constraint: r.Constraint})
	}

	return out
}
