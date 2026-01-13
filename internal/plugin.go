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
	Description  string       `yaml:",omitempty"`
	Env          []EnvVar     `yaml:",omitempty"`
	Flags        []Flag       `yaml:",omitempty"`
	Requirements Requirements `yaml:",omitempty"`
}

// EnvVar represents an environment variable required by the plugin
type EnvVar struct {
	Name string
}

// Flag represents a command-line flag supported by the plugin
type Flag struct {
	Name string
}

// Requirements represents plugin dependencies
type Requirements struct {
	Kubernetes KubernetesRequirement `yaml:",omitempty"`
	Modules    []ModuleRequirement   `yaml:",omitempty"`
	Plugins    []PluginRequirement   `yaml:",omitempty"`
}

// KubernetesRequirement represents Kubernetes version constraint
type KubernetesRequirement struct {
	Constraint string `yaml:",omitempty"`
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
