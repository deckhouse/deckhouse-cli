package templates

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

type Options struct {
	Data    any
	Exclude []string
}

// Render walks the filesystem and renders each file to the output directory.
// Files with the .tmpl extension are executed as Go templates (the extension is stripped
// from the output filename). All other files are copied as-is.
func Render(templateFS fs.FS, output string, opts Options) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	return fs.WalkDir(templateFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if isExcluded(path, opts.Exclude) {
			if d.IsDir() {
				return fs.SkipDir
			}

			return nil
		}

		if d.IsDir() {
			return nil
		}

		out := filepath.Join(output, path)

		// Ensure parent directory exists
		if err = os.MkdirAll(filepath.Dir(out), 0755); err != nil {
			return fmt.Errorf("failed to create parent directory: %w", err)
		}

		content, readErr := fs.ReadFile(templateFS, path)
		if readErr != nil {
			return fmt.Errorf("failed to read embedded file: %w", readErr)
		}

		// Only render .tmpl files as Go templates, copy everything else as-is
		if strings.HasSuffix(path, ".tmpl") {
			out = strings.TrimSuffix(out, ".tmpl")
			return renderTemplate(path, content, out, opts.Data)
		}

		if err = os.WriteFile(out, content, 0644); err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}

		return nil
	})
}

// Clean removes all files and empty directories that Render would produce from the output directory.
// After removing each file, it attempts to remove parent directories up to the output root.
// Non-empty directories are silently skipped.
func Clean(templateFS fs.FS, output string) error {
	return fs.WalkDir(templateFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		out := filepath.Join(output, path)
		if strings.HasSuffix(path, ".tmpl") {
			out = strings.TrimSuffix(out, ".tmpl")
		}

		if err = os.Remove(out); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove file %s: %w", out, err)
		}

		// Walk up and remove empty parent directories until output root
		for dir := filepath.Dir(out); dir != output; dir = filepath.Dir(dir) {
			if os.Remove(dir) != nil {
				break
			}
		}

		return nil
	})
}

// isExcluded checks if a path matches any of the exclude patterns.
// Patterns are matched against the path using filepath.Match.
func isExcluded(path string, patterns []string) bool {
	for _, pattern := range patterns {
		if matched, _ := filepath.Match(pattern, filepath.Base(path)); matched {
			return true
		}
	}

	return false
}

// renderTemplate parses and executes content as a Go template, writing the result to destPath.
// Returns an error if parsing or execution fails — no silent fallback.
func renderTemplate(name string, content []byte, destPath string, opts any) error {
	tmpl, err := template.New(filepath.Base(name)).
		Option("missingkey=zero").
		Parse(string(content))
	if err != nil {
		return fmt.Errorf("failed to parse template %s: %w", name, err)
	}

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, opts); err != nil {
		return fmt.Errorf("failed to execute template %s: %w", name, err)
	}

	if err = os.WriteFile(destPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}
