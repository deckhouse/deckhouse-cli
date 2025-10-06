package plugins

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

// RequirementsDTO represents requirements in JSON
type RequirementsDTO struct {
	Kubernetes KubernetesRequirementDTO `json:"kubernetes,omitempty"`
	Modules    []ModuleRequirementDTO   `json:"modules,omitempty"`
}

// KubernetesRequirementDTO represents Kubernetes requirement in JSON
type KubernetesRequirementDTO struct {
	Constraint string `json:"constraint"`
}

// ModuleRequirementDTO represents module requirement in JSON
type ModuleRequirementDTO struct {
	Name       string `json:"name"`
	Constraint string `json:"constraint"`
}
