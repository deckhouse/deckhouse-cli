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

package farconverter

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/gosimple/slug"
	"github.com/iancoleman/strcase"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// RawRule represents decoded rules in Falco format.
type RawRule map[string]any

// FalcoAuditRule is a structure to encode FalcoAuditRules custom resources.
type FalcoAuditRule struct {
	ApiVersion string `yaml:"apiVersion"`
	Kind       string
	Metadata   Metadata
	Spec       FalcoAuditRuleSpec
}

type FalcoAuditRuleSpecRule struct {
	Rule  Rule  `yaml:"rule,omitempty"`
	Macro Macro `yaml:"macro,omitempty"`
	List  List  `yaml:"list,omitempty"`
}

type FalcoAuditRuleSpec struct {
	RequiredEngineVersion         int                      `yaml:"requiredEngineVersion,omitempty"`
	RequiredK8sAuditPluginVersion string                   `yaml:"requiredK8sAuditPluginVersion,omitempty"`
	Rules                         []FalcoAuditRuleSpecRule `yaml:"rules"`
}

type Metadata struct {
	Name string
}

type Rule struct {
	Name      string `yaml:"name"`
	Condition string `yaml:"condition"`
	Desc      string `yaml:"desc"`
	Output    string `yaml:"output"`
	Priority  string `yaml:"priority"`
	Enabled   bool   `yaml:"enabled,omitempty"`
	Tags      []any  `yaml:"tags,omitempty"`
	Source    string `yaml:"source,omitempty"`
}

type Macro struct {
	Name      string `yaml:"name"`
	Condition string `yaml:"condition"`
}

type List struct {
	Name  string `yaml:"name"`
	Items []any  `yaml:"items"`
}

func Convert(cmd *cobra.Command, args []string) error {
	input := args[0]
	var rules []RawRule
	log.Printf("Convert rules from %q", input)

	data, err := os.ReadFile(input)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(data, &rules)
	if err != nil {
		return err
	}

	buf := bytes.Buffer{}
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)

	if err := enc.Encode(convert(input, rules)); err != nil {
		return err
	}

	if _, err = buf.WriteTo(os.Stdout); err != nil {
		return err
	}
	return nil
}

func convert(path string, rules []RawRule) FalcoAuditRule {
	result := FalcoAuditRule{
		ApiVersion: "deckhouse.io/v1alpha1",
		Kind:       "FalcoAuditRules",
		Metadata: Metadata{
			Name: nameFromPath(path),
		},
		Spec: FalcoAuditRuleSpec{},
	}

	for _, r := range rules {
		if v, ok := r["required_engine_version"]; ok {
			result.Spec.RequiredEngineVersion = v.(int)
			continue
		}

		if v, ok := r["required_plugin_versions"]; ok {
			for _, p := range v.([]any) {
				plugin := p.(map[string]any)
				if plugin["name"].(string) == "k8saudit" {
					result.Spec.RequiredK8sAuditPluginVersion = plugin["version"].(string)
				}
			}
			continue
		}

		if _, ok := r["macro"]; ok {
			result.Spec.Rules = append(result.Spec.Rules, FalcoAuditRuleSpecRule{
				Macro: Macro{
					Name:      r["macro"].(string),
					Condition: r["condition"].(string),
				},
			})
			continue
		}

		if _, ok := r["list"]; ok {
			result.Spec.Rules = append(result.Spec.Rules, FalcoAuditRuleSpecRule{
				List: List{
					Name:  r["list"].(string),
					Items: r["items"].([]any),
				},
			})
			continue
		}

		if _, ok := r["rule"]; ok {
			ruleToAdd := Rule{
				Name:      r["rule"].(string),
				Condition: r["condition"].(string),
				Desc:      r["desc"].(string),
				Output:    r["output"].(string),
				Priority:  strcase.ToCamel(strings.ToLower(r["priority"].(string))),
			}

			if tags, ok := r["tags"]; ok {
				ruleToAdd.Tags = tags.([]any)
			}

			if enabled, ok := r["enabled"]; ok {
				ruleToAdd.Enabled = enabled.(bool)
			}

			if source, ok := r["source"]; ok {
				switch strings.ToLower(source.(string)) {
				case "k8s_audit":
					ruleToAdd.Source = "K8sAudit"
				case "syscall":
					ruleToAdd.Source = "Syscall"
				}
			}

			if _, ok := r["exceptions"]; ok {
				log.Printf("[WARNING] Exceptions are not supported (found in %q rule)", ruleToAdd.Name)
			}

			result.Spec.Rules = append(result.Spec.Rules, FalcoAuditRuleSpecRule{Rule: ruleToAdd})
			continue
		}
	}

	return result
}

func nameFromPath(path string) string {
	path = filepath.Base(path)
	path, _, _ = strings.Cut(path, ".")
	path = slug.Make(path)
	path = strcase.ToKebab(path)
	return path
}
