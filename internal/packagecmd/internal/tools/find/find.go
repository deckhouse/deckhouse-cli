package find

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
)

// PackageDir recursively searches for a directory containing package.yaml.
// It starts from the current working directory and searches it first,
// then recursively searches all subdirectories.
// Returns the absolute path to the directory containing package.yaml.
func PackageDir() (string, error) {
	// Get current working directory
	currentDir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get current working directory: %w", err)
	}

	// Check current directory first
	packagePath := filepath.Join(currentDir, packages.DefinitionFile)
	if _, err = os.Stat(packagePath); err == nil {
		return currentDir, nil
	}

	// Recursively search subdirectories
	var foundPath string

	err = filepath.Walk(currentDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip directories we can't access
		}

		// Skip embed templates
		if info.Name() == "templates" && info.IsDir() {
			return filepath.SkipDir
		}

		// Skip if already found
		if foundPath != "" {
			return filepath.SkipDir
		}

		// Check if this directory contains package.yaml
		if info.IsDir() {
			packagePath = filepath.Join(path, packages.DefinitionFile)
			if _, err = os.Stat(packagePath); err == nil {
				foundPath = path
				return filepath.SkipDir
			}
		}

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk directory tree: %w", err)
	}

	if foundPath == "" {
		return "", fmt.Errorf("package.yaml not found")
	}

	// Ensure we return an absolute path
	absPath, err := filepath.Abs(foundPath)
	if err != nil {
		return "", fmt.Errorf("get absolute path: %w", err)
	}

	return absPath, nil
}
