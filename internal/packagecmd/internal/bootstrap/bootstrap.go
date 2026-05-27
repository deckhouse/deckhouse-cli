package bootstrap

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/logs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/templates"
)

const (
	hooksDir = "hooks"
	ossFile  = "oss.yaml"
)

type Options struct {
	GenerateHook bool
	Extended     bool
	Werf         bool
	Data         Data
}

type Data struct {
	PackageType string
	PackageName string
}

// Bootstrap copies and renders all embedded template files with the provided data.
// Files are parsed as Go templates and executed with the given data structure.
func Bootstrap(out string, opts Options, logger *logs.Logger) error {
	if out == "" {
		return fmt.Errorf("output path cannot be empty")
	}

	logger.Info("✨ Bootstrap package '%s' (%s) to %s", opts.Data.PackageName, opts.Data.PackageType, out)

	fs := templates.ApplicationFS
	if opts.Data.PackageType == packages.TypeModule {
		fs = templates.ModuleFS
	}

	var exclude []string

	if !opts.Extended {
		exclude = append(exclude, ossFile)
	}

	if !opts.GenerateHook {
		exclude = append(exclude, hooksDir)
	}

	if opts.Werf {
		exclude = append(exclude, "Dockerfile")
	} else {
		exclude = append(exclude, "werf.inc.yaml")
	}

	renderOpts := templates.Options{
		Data:    opts.Data,
		Exclude: exclude,
	}

	if err := templates.Render(fs, out, renderOpts); err != nil {
		return fmt.Errorf("failed to render templates: %w", err)
	}

	logger.Info("✨ Initialize git repository...")

	// Initialize git repository and make initial commit
	if err := initGitRepo(out); err != nil {
		return fmt.Errorf("failed to initialize git repository: %w", err)
	}

	logger.Info("✅ Package bootstrapped successfully")

	return nil
}

// initGitRepo initializes a git repository and makes an initial commit
func initGitRepo(dir string) error {
	// Check if git is already initialized
	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		// Git repo already exists, skip initialization
		return nil
	}

	// Initialize git repository
	cmd := exec.Command("git", "init", "--initial-branch=main")

	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git init failed: %w", err)
	}

	// Add all files
	cmd = exec.Command("git", "add", ".")

	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git add failed: %w", err)
	}

	// Make initial commit
	cmd = exec.Command("git", "commit", "-m", "Initial commit")

	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git commit failed: %w", err)
	}

	return nil
}
