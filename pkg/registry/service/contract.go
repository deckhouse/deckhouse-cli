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

	"github.com/Masterminds/semver/v3"

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

// ContractToDomain converts a PluginContract DTO to a Plugin domain entity and
// validates its module requirement groups (see validateModuleRequirements). An
// ill-formed contract is rejected rather than silently yielding a plugin whose
// requirements cannot be enforced.
func ContractToDomain(contract *PluginContract) (*internal.Plugin, error) {
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

	if err := validateModuleRequirements(plugin.Requirements.Modules); err != nil {
		return nil, err
	}

	return plugin, nil
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
	return internal.ModuleRequirementsGroup{
		Mandatory:   moduleReqsToDomain(g.Mandatory),
		Conditional: moduleReqsToDomain(g.Conditional),
		AnyOf:       moduleGroupsToDomain(g.AnyOf),
		NoneOf:      moduleGroupsToDomain(g.NoneOf),
	}
}

func moduleGroupToDTO(g internal.ModuleRequirementsGroup) ModuleRequirementsGroupDTO {
	return ModuleRequirementsGroupDTO{
		Mandatory:   moduleReqsToDTO(g.Mandatory),
		Conditional: moduleReqsToDTO(g.Conditional),
		AnyOf:       moduleGroupsToDTO(g.AnyOf),
		NoneOf:      moduleGroupsToDTO(g.NoneOf),
	}
}

func moduleGroupsToDomain(groups []ModuleGroupDTO) []internal.ModuleGroup {
	out := make([]internal.ModuleGroup, 0, len(groups))
	for _, grp := range groups {
		out = append(out, internal.ModuleGroup{
			Name:        grp.Name,
			Description: grp.Description,
			Modules:     moduleReqsToDomain(grp.Modules),
		})
	}

	return out
}

func moduleGroupsToDTO(groups []internal.ModuleGroup) []ModuleGroupDTO {
	out := make([]ModuleGroupDTO, 0, len(groups))
	for _, grp := range groups {
		out = append(out, ModuleGroupDTO{
			Name:        grp.Name,
			Description: grp.Description,
			Modules:     moduleReqsToDTO(grp.Modules),
		})
	}

	return out
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

// validateModuleRequirements checks contract well-formedness for the module
// requirement groups, mirroring what the Deckhouse controller applies to a
// package.yaml (buildModuleGroups + validateBucketCollisions): each anyOf/noneOf
// group needs a unique, non-empty name and at least one member; members are
// unique within a group and carry valid semver constraints; and no module may
// appear in contradictory buckets.
func validateModuleRequirements(m internal.ModuleRequirementsGroup) error {
	mandatory := moduleNameSet(m.Mandatory)
	conditional := moduleNameSet(m.Conditional)

	for name := range conditional {
		if _, dup := mandatory[name]; dup {
			return fmt.Errorf("module %q appears in both mandatory and conditional", name)
		}
	}

	anyOfMembers, err := validateModuleGroups(m.AnyOf, "anyOf")
	if err != nil {
		return err
	}

	noneOfMembers, err := validateModuleGroups(m.NoneOf, "noneOf")
	if err != nil {
		return err
	}

	for member, group := range anyOfMembers {
		if _, dup := mandatory[member]; dup {
			return fmt.Errorf("module %q appears in both mandatory and anyOf group %q", member, group)
		}

		if _, dup := conditional[member]; dup {
			return fmt.Errorf("module %q appears in both conditional and anyOf group %q", member, group)
		}
	}

	for member, group := range noneOfMembers {
		if _, dup := mandatory[member]; dup {
			return fmt.Errorf("module %q appears in both mandatory and noneOf group %q", member, group)
		}

		if _, dup := conditional[member]; dup {
			return fmt.Errorf("module %q appears in both conditional and noneOf group %q", member, group)
		}

		if anyGroup, dup := anyOfMembers[member]; dup {
			return fmt.Errorf("module %q appears in both anyOf group %q and noneOf group %q", member, anyGroup, group)
		}
	}

	return nil
}

// validateModuleGroups validates one bucket of anyOf/noneOf groups and returns a
// map of member module name to the group that declares it, for cross-bucket
// collision checks. bucket ("anyOf"/"noneOf") is woven into error messages. The
// same module across two distinct groups of one bucket is allowed.
func validateModuleGroups(groups []internal.ModuleGroup, bucket string) (map[string]string, error) {
	seenGroups := make(map[string]struct{}, len(groups))
	members := make(map[string]string)

	for i, group := range groups {
		if group.Name == "" {
			return nil, fmt.Errorf("%s group [%d]: name is required", bucket, i)
		}

		if _, dup := seenGroups[group.Name]; dup {
			return nil, fmt.Errorf("%s group %q: duplicate group name", bucket, group.Name)
		}

		seenGroups[group.Name] = struct{}{}

		if len(group.Modules) == 0 {
			return nil, fmt.Errorf("%s group %q: at least one member is required", bucket, group.Name)
		}

		seenMembers := make(map[string]struct{}, len(group.Modules))
		for _, member := range group.Modules {
			if member.Name == "" {
				return nil, fmt.Errorf("%s group %q: member name is required", bucket, group.Name)
			}

			if _, dup := seenMembers[member.Name]; dup {
				return nil, fmt.Errorf("%s group %q: duplicate member %q", bucket, group.Name, member.Name)
			}

			seenMembers[member.Name] = struct{}{}

			if member.Constraint != "" {
				if _, err := semver.NewConstraint(member.Constraint); err != nil {
					return nil, fmt.Errorf("%s group %q member %q: invalid constraint %q: %w",
						bucket, group.Name, member.Name, member.Constraint, err)
				}
			}

			members[member.Name] = group.Name
		}
	}

	return members, nil
}

func moduleNameSet(reqs []internal.ModuleRequirement) map[string]struct{} {
	out := make(map[string]struct{}, len(reqs))
	for _, r := range reqs {
		out[r.Name] = struct{}{}
	}

	return out
}
