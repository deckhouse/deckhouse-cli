package rules

import (
	"os"
	"path/filepath"
	"strings"
)

// collectFiles returns files under root whose names have one of the provided extensions.
func collectFiles(root string, extensions ...string) ([]string, error) {
	files := make([]string, 0)

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		for _, ext := range extensions {
			if strings.HasSuffix(d.Name(), ext) {
				files = append(files, path)
				return nil
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}

// packageRelativePath returns fullPath relative to packagePath, falling back to fullPath on error.
func packageRelativePath(packagePath, fullPath string) string {
	relPath, err := filepath.Rel(packagePath, fullPath)
	if err != nil {
		return fullPath
	}

	return filepath.ToSlash(relPath)
}
