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

// Requirements represents plugin dependencies
type Requirements struct {
	Kubernetes KubernetesRequirement
	Modules    []ModuleRequirement
}

// KubernetesRequirement represents Kubernetes version constraint
type KubernetesRequirement struct {
	Constraint string
}

// ModuleRequirement represents a required Deckhouse module
type ModuleRequirement struct {
	Name       string
	Constraint string
}
