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
	"strings"
	"testing"
)

// TestPluginContract_V2FieldsParsed: a full v2 contract must populate every
// new top-level field (deckhouse, plugins.{mandatory,conditional},
// modules.{mandatory,conditional,anyOf,noneOf}). Catches structural breakage of
// any new DTO field with one assertion per section.
func TestPluginContract_V2FieldsParsed(t *testing.T) {
	in := []byte(`{
		"name":"stronghold","version":"v1.2.3","description":"x",
		"requirements":{
			"deckhouse":{"constraint":">=1.76"},
			"plugins":  {"mandatory":[{"name":"delivery","constraint":">=1.0.0"}],
			             "conditional":[{"name":"iam","constraint":">=1.0.0"}]},
			"modules":  {"mandatory":[{"name":"stronghold","constraint":">=1.0.0"}],
			             "conditional":[{"name":"observability","constraint":">=1.0.0"}],
			             "anyOf":[{"name":"cni","description":"cni","modules":[{"name":"cni-flannel","constraint":">=1.5.0"}]}],
			             "noneOf":[{"name":"legacy","description":"legacy","modules":[{"name":"cni-simple-bridge","constraint":"<1.0.0"}]}]}
		}
	}`)
	var c PluginContract
	if err := json.Unmarshal(in, &c); err != nil {
		t.Fatalf("unmarshal v2: %v", err)
	}
	if c.Requirements.Deckhouse.Constraint != ">=1.76" {
		t.Errorf("deckhouse not parsed: %+v", c.Requirements.Deckhouse)
	}
	if len(c.Requirements.Plugins.Mandatory) != 1 || len(c.Requirements.Plugins.Conditional) != 1 {
		t.Errorf("plugins not split: %+v", c.Requirements.Plugins)
	}
	if len(c.Requirements.Modules.Mandatory) != 1 || len(c.Requirements.Modules.Conditional) != 1 {
		t.Errorf("modules.mandatory/conditional not parsed: %+v", c.Requirements.Modules)
	}
	if len(c.Requirements.Modules.AnyOf) != 1 || len(c.Requirements.Modules.AnyOf[0].Modules) != 1 {
		t.Errorf("modules.anyOf not parsed: %+v", c.Requirements.Modules.AnyOf)
	}
	if c.Requirements.Modules.AnyOf[0].Name != "cni" {
		t.Errorf("modules.anyOf[0].name not parsed: %+v", c.Requirements.Modules.AnyOf)
	}
	if len(c.Requirements.Modules.NoneOf) != 1 || len(c.Requirements.Modules.NoneOf[0].Modules) != 1 {
		t.Errorf("modules.noneOf not parsed: %+v", c.Requirements.Modules.NoneOf)
	}
	if c.Requirements.Modules.NoneOf[0].Name != "legacy" {
		t.Errorf("modules.noneOf[0].name not parsed: %+v", c.Requirements.Modules.NoneOf)
	}
}

// TestPluginContract_FlatArrayRejected: flat-array form for
// requirements.modules / requirements.plugins isn't part of the schema
// and must surface as an unmarshal error rather than silently coercing
// into one of the mandatory/conditional sections.
func TestPluginContract_FlatArrayRejected(t *testing.T) {
	in := []byte(`{
		"name":"x","version":"v1.0.0","description":"x",
		"requirements":{
			"modules":[{"name":"m","constraint":">=1.0.0"}],
			"plugins":[{"name":"p","constraint":">=1.0.0"}]
		}
	}`)
	var c PluginContract
	if err := json.Unmarshal(in, &c); err == nil {
		t.Fatal("expected unmarshal error for flat-array contract, got nil")
	}
}

// A flat-array contract must produce a user-actionable error.
// The message names the offending field and the expected shape,
// not the raw encoding/json type error.
// UnmarshalContract is the shared path for OCI annotations,
// file loads, and registry-packages-proxy.
func TestUnmarshalContract_FriendlyArrayMessage(t *testing.T) {
	in := []byte(`{
		"name":"x","version":"v1.0.0",
		"requirements":{"modules":[{"name":"m","constraint":">=1.0.0"}]}
	}`)
	var c PluginContract
	err := UnmarshalContract(in, &c)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := err.Error()
	for _, want := range []string{
		"invalid contract",
		`"requirements.modules"`,
		"mandatory/conditional sections",
		"got a JSON array",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q\ngot: %s", want, msg)
		}
	}
	if strings.Contains(msg, "ModuleRequirementsGroupDTO") {
		t.Errorf("error message still leaks internal Go type name:\n%s", msg)
	}
}

// TestContractToDomain_CarriesModuleGroups: a valid contract's anyOf and noneOf
// groups must reach the domain plugin the checker consumes, with name, member
// name, and constraint intact. Guards the parse -> domain link that the
// enforcement tests (which build the domain directly) do not exercise.
func TestContractToDomain_CarriesModuleGroups(t *testing.T) {
	plugin, err := ContractToDomain(&PluginContract{
		Name:    "p",
		Version: "v1.0.0",
		Requirements: RequirementsDTO{Modules: ModuleRequirementsGroupDTO{
			AnyOf: []ModuleGroupDTO{{
				Name:    "cni",
				Modules: []ModuleRequirementDTO{{Name: "cni-cilium", Constraint: ">= 1.0"}},
			}},
			NoneOf: []ModuleGroupDTO{{
				Name:    "legacy",
				Modules: []ModuleRequirementDTO{{Name: "cni-flannel", Constraint: "< 1.0"}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("ContractToDomain: %v", err)
	}

	anyOf := plugin.Requirements.Modules.AnyOf
	if len(anyOf) != 1 || anyOf[0].Name != "cni" ||
		len(anyOf[0].Modules) != 1 || anyOf[0].Modules[0].Name != "cni-cilium" ||
		anyOf[0].Modules[0].Constraint != ">= 1.0" {
		t.Errorf("anyOf not carried into domain: %+v", anyOf)
	}

	noneOf := plugin.Requirements.Modules.NoneOf
	if len(noneOf) != 1 || noneOf[0].Name != "legacy" ||
		len(noneOf[0].Modules) != 1 || noneOf[0].Modules[0].Name != "cni-flannel" ||
		noneOf[0].Modules[0].Constraint != "< 1.0" {
		t.Errorf("noneOf not carried into domain: %+v", noneOf)
	}
}

// TestContractToDomain_RejectsInvalidGroups: ContractToDomain must reject an
// ill-formed contract, and the error must name the violated rule. Asserting the
// message pins each case to its rule instead of just "some error occurred".
func TestContractToDomain_RejectsInvalidGroups(t *testing.T) {
	tests := []struct {
		name        string
		giveModules ModuleRequirementsGroupDTO
		wantMsg     string
	}{
		{
			name:        "group without name",
			giveModules: ModuleRequirementsGroupDTO{NoneOf: []ModuleGroupDTO{{Modules: []ModuleRequirementDTO{{Name: "m"}}}}},
			wantMsg:     "name is required",
		},
		{
			name: "duplicate group name",
			giveModules: ModuleRequirementsGroupDTO{AnyOf: []ModuleGroupDTO{
				{Name: "g", Modules: []ModuleRequirementDTO{{Name: "a"}}},
				{Name: "g", Modules: []ModuleRequirementDTO{{Name: "b"}}},
			}},
			wantMsg: "duplicate group name",
		},
		{
			name: "module in both anyOf and noneOf",
			giveModules: ModuleRequirementsGroupDTO{
				AnyOf:  []ModuleGroupDTO{{Name: "a", Modules: []ModuleRequirementDTO{{Name: "shared"}}}},
				NoneOf: []ModuleGroupDTO{{Name: "n", Modules: []ModuleRequirementDTO{{Name: "shared"}}}},
			},
			wantMsg: "both anyOf group",
		},
		{
			name: "noneOf member also mandatory",
			giveModules: ModuleRequirementsGroupDTO{
				Mandatory: []ModuleRequirementDTO{{Name: "shared"}},
				NoneOf:    []ModuleGroupDTO{{Name: "n", Modules: []ModuleRequirementDTO{{Name: "shared"}}}},
			},
			wantMsg: "both mandatory and noneOf group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ContractToDomain(&PluginContract{
				Name:         "p",
				Version:      "v1.0.0",
				Requirements: RequirementsDTO{Modules: tt.giveModules},
			})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}

			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q does not mention %q", err.Error(), tt.wantMsg)
			}
		})
	}
}
