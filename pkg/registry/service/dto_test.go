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

// TestSmartUnmarshal_AcceptsV1FlatArray: legacy v1 contracts use a flat
// array for plugins/modules requirements. Smart-unmarshal must place those
// into .Mandatory; without this, every existing plugin in the registry breaks.
func TestSmartUnmarshal_AcceptsV1FlatArray(t *testing.T) {
	var g PluginRequirementsGroupDTO
	if err := json.Unmarshal([]byte(`[{"name":"foo","constraint":">=1.0.0"}]`), &g); err != nil {
		t.Fatalf("unmarshal v1 array: %v", err)
	}
	if len(g.Mandatory) != 1 || g.Mandatory[0].Name != "foo" {
		t.Errorf("v1 array did not land in .Mandatory: %+v", g)
	}
	if len(g.Conditional) != 0 {
		t.Errorf(".Conditional should be empty for v1 input: %+v", g)
	}
}

// TestSmartUnmarshal_AcceptsV2Object: v2 contracts use a struct with named
// keys. Smart-unmarshal must parse each section into the right field. Uses
// the module group because anyOf is the most complex shape.
func TestSmartUnmarshal_AcceptsV2Object(t *testing.T) {
	in := []byte(`{
		"mandatory":   [{"name":"stronghold","constraint":">=1.0.0"}],
		"conditional": [{"name":"observability","constraint":">=1.0.0"}],
		"anyOf": [{"description":"cloud provider","modules":[{"name":"cloud-provider-aws","constraint":">=2.0.0"}]}]
	}`)
	var g ModuleRequirementsGroupDTO
	if err := json.Unmarshal(in, &g); err != nil {
		t.Fatalf("unmarshal v2 object: %v", err)
	}
	if len(g.Mandatory) != 1 || len(g.Conditional) != 1 || len(g.AnyOf) != 1 {
		t.Fatalf("v2 sections not all parsed: %+v", g)
	}
	if g.AnyOf[0].Description != "cloud provider" || len(g.AnyOf[0].Modules) != 1 {
		t.Errorf("anyOf group content lost: %+v", g.AnyOf[0])
	}
}

// TestPluginContract_V1RoundTripsToV2: a v1 contract from the registry must
// parse successfully AND on subsequent marshal produce v2 keys, so the on-disk
// cache and `d8 plugins contract` output gradually migrate to v2.
func TestPluginContract_V1RoundTripsToV2(t *testing.T) {
	v1 := []byte(`{
		"name":"legacy","version":"v1.0.0","description":"x",
		"requirements":{
			"modules":[{"name":"m","constraint":">=1.0.0"}],
			"plugins":[{"name":"p","constraint":">=1.0.0"}]
		}
	}`)
	var c PluginContract
	if err := json.Unmarshal(v1, &c); err != nil {
		t.Fatalf("unmarshal v1: %v", err)
	}
	if len(c.Requirements.Modules.Mandatory) != 1 || len(c.Requirements.Plugins.Mandatory) != 1 {
		t.Fatalf("v1 arrays did not normalize into .Mandatory: %+v", c.Requirements)
	}
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), `"mandatory"`) {
		t.Errorf("expected v2 'mandatory' key in marshalled output, got: %s", out)
	}
}

// TestPluginContract_V2NewFieldsPopulated: a full v2 contract must populate
// the new top-level deckhouse requirement and the structured group sections.
func TestPluginContract_V2NewFieldsPopulated(t *testing.T) {
	v2 := []byte(`{
		"name":"stronghold","version":"v1.2.3","description":"x",
		"requirements":{
			"deckhouse":{"constraint":">=1.76"},
			"plugins":  {"mandatory":[{"name":"delivery","constraint":">=1.0.0"}],
			             "conditional":[{"name":"iam","constraint":">=1.0.0"}]},
			"modules":  {"mandatory":[{"name":"stronghold","constraint":">=1.0.0"}],
			             "anyOf":[{"description":"cni","modules":[{"name":"cni-flannel","constraint":">=1.5.0"}]}]}
		}
	}`)
	var c PluginContract
	if err := json.Unmarshal(v2, &c); err != nil {
		t.Fatalf("unmarshal v2: %v", err)
	}
	if c.Requirements.Deckhouse.Constraint != ">=1.76" {
		t.Errorf("deckhouse constraint missing: %+v", c.Requirements.Deckhouse)
	}
	if len(c.Requirements.Plugins.Conditional) != 1 {
		t.Errorf("plugins.conditional not parsed: %+v", c.Requirements.Plugins)
	}
	if len(c.Requirements.Modules.AnyOf) != 1 {
		t.Errorf("modules.anyOf not parsed: %+v", c.Requirements.Modules)
	}
}
