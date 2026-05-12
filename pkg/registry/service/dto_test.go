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
	"testing"
)

// TestPluginContract_V2FieldsParsed: a full v2 contract must populate every
// new top-level field (deckhouse, plugins.{mandatory,conditional},
// modules.{mandatory,conditional,anyOf}). Catches structural breakage of
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
			             "anyOf":[{"description":"cni","modules":[{"name":"cni-flannel","constraint":">=1.5.0"}]}]}
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
}

// TestPluginContract_LegacyV1Rejected: flat-array form for plugins/modules is
// no longer supported. Any contract still using it must surface a clear
// unmarshal error rather than silently coercing into one of the v2 sections.
func TestPluginContract_LegacyV1Rejected(t *testing.T) {
	in := []byte(`{
		"name":"legacy","version":"v1.0.0","description":"x",
		"requirements":{
			"modules":[{"name":"m","constraint":">=1.0.0"}],
			"plugins":[{"name":"p","constraint":">=1.0.0"}]
		}
	}`)
	var c PluginContract
	if err := json.Unmarshal(in, &c); err == nil {
		t.Fatal("expected unmarshal error for v1 flat-array contract, got nil")
	}
}
