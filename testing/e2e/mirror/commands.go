/*
Copyright 2024 Flant JSC

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

package mirror

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

// =============================================================================
// Command Builders
// =============================================================================

// buildPullCommand builds the d8 mirror pull command
func buildPullCommand(cfg *Config, bundleDir string) *exec.Cmd {
	args := []string{
		"mirror", "pull",
		"--source", cfg.SourceRegistry,
		"--force", // overwrite if exists
	}

	// Authentication
	if cfg.SourceUser != "" {
		args = append(args, "--source-login", cfg.SourceUser)
		args = append(args, "--source-password", cfg.SourcePassword)
	} else if cfg.LicenseToken != "" {
		args = append(args, "--license", cfg.LicenseToken)
	}

	// TLS
	if cfg.TLSSkipVerify {
		args = append(args, "--tls-skip-verify")
	}

	// Debug options
	if cfg.NoModules {
		args = append(args, "--no-modules")
	}

	args = append(args, bundleDir)

	cmd := exec.Command(cfg.D8Binary, args...)
	cmd.Env = append(os.Environ(), "HOME="+os.Getenv("HOME"))

	// Experimental options (via env)
	if cfg.NewPull {
		cmd.Env = append(cmd.Env, "NEW_PULL=true")
	}

	return cmd
}

// buildPushCommand builds the d8 mirror push command
func buildPushCommand(cfg *Config, bundleDir, targetRegistry string) *exec.Cmd {
	args := []string{
		"mirror", "push",
		bundleDir,
		targetRegistry,
	}

	// TLS
	if cfg.TLSSkipVerify {
		args = append(args, "--tls-skip-verify")
	}

	// Authentication
	if cfg.TargetUser != "" {
		args = append(args, "--registry-login", cfg.TargetUser)
		args = append(args, "--registry-password", cfg.TargetPassword)
	}

	cmd := exec.Command(cfg.D8Binary, args...)
	cmd.Env = append(os.Environ(), "HOME="+os.Getenv("HOME"))

	return cmd
}

// =============================================================================
// Command Runner
// =============================================================================

// runCommandWithLog runs command with streaming output and saves to log file
func runCommandWithLog(t *testing.T, cmd *exec.Cmd, logFile string) error {
	t.Helper()

	// Open log file
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("Warning: could not open log file %s: %v", logFile, err)
		// Fallback: just stream without logging
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}
	defer f.Close()

	// Write command header
	fmt.Fprintf(f, "\n\n========== COMMAND: %s ==========\n", cmd.String())
	fmt.Fprintf(f, "Started: %s\n\n", time.Now().Format(time.RFC3339))

	// Stream to stdout and file
	cmd.Stdout = io.MultiWriter(os.Stdout, f)
	cmd.Stderr = io.MultiWriter(os.Stderr, f)

	// Run
	cmdErr := cmd.Run()

	// Write result
	if cmdErr != nil {
		fmt.Fprintf(f, "\n\n========== COMMAND FAILED: %v ==========\n", cmdErr)
	} else {
		fmt.Fprintf(f, "\n\n========== COMMAND SUCCEEDED ==========\n")
	}

	return cmdErr
}
