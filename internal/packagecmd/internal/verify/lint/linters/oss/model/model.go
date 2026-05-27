package model

import "fmt"

// Component describes one oss.yaml component entry.
type Component struct {
	ID          string             `yaml:"id"`
	Name        string             `yaml:"name"`
	Description string             `yaml:"description"`
	Link        string             `yaml:"link"`
	License     string             `yaml:"license"`
	Version     string             `yaml:"version"`
	Versions    []ComponentVersion `yaml:"versions"`
}

// ComponentVersion describes one entry in component versions.
type ComponentVersion struct {
	Name      string            `yaml:"name"`
	Version   string            `yaml:"version"`
	Condition map[string]string `yaml:"condition"`
}

// ComponentObjectID returns a stable diagnostic object identifier for one component.
func ComponentObjectID(index int, component Component) string {
	if component.ID != "" {
		return component.ID
	}

	return fmt.Sprintf("component[%d]", index)
}
