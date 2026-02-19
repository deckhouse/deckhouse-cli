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

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

func BuildPullCommand(cfg *Config, bundleDir string) *exec.Cmd {
	args := []string{
		"mirror", "pull",
		"--source", cfg.SourceRegistry,
		"--force",
	}

	if cfg.SourceUser != "" {
		args = append(args, "--source-login", cfg.SourceUser)
		args = append(args, "--source-password", cfg.SourcePassword)
	} else if cfg.LicenseToken != "" {
		args = append(args, "--license", cfg.LicenseToken)
	}

	if cfg.TLSSkipVerify {
		args = append(args, "--tls-skip-verify")
	}

	if cfg.DeckhouseTag != "" {
		args = append(args, "--deckhouse-tag", cfg.DeckhouseTag)
	}
	if cfg.NoModules {
		args = append(args, "--no-modules")
	}
	if cfg.NoPlatform {
		args = append(args, "--no-platform")
	}
	if cfg.NoSecurity {
		args = append(args, "--no-security-db")
	}
	for _, module := range cfg.IncludeModules {
		args = append(args, "--include-module", module)
	}

	args = append(args, bundleDir)

	cmd := exec.Command(cfg.D8Binary, args...)
	cmd.Env = os.Environ()

	if cfg.NewPull {
		cmd.Env = append(cmd.Env, "NEW_PULL=true")
	}

	return cmd
}

func BuildPushCommand(cfg *Config, bundleDir, targetRegistry string) *exec.Cmd {
	args := []string{
		"mirror", "push",
		bundleDir,
		targetRegistry,
	}

	if cfg.TLSSkipVerify {
		args = append(args, "--tls-skip-verify")
	}

	if cfg.TargetUser != "" {
		args = append(args, "--registry-login", cfg.TargetUser)
		args = append(args, "--registry-password", cfg.TargetPassword)
	}

	cmd := exec.Command(cfg.D8Binary, args...)
	cmd.Env = os.Environ()
	
	// Ensure HOME is set - some tools (like ssh) require it
	if os.Getenv("HOME") == "" {
		if homeDir, err := os.UserHomeDir(); err == nil {
			cmd.Env = append(cmd.Env, "HOME="+homeDir)
		}
	}

	if cfg.NewPull {
		cmd.Env = append(cmd.Env, "NEW_PULL=true")
	}

	return cmd
}

func RunCommandWithLog(t *testing.T, cmd *exec.Cmd, logFile string) error {
	t.Helper()

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("Warning: could not open log file %s: %v", logFile, err)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}
	defer f.Close()

	fmt.Fprintf(f, "\n\n========== COMMAND: %s ==========\n", cmd.String())
	fmt.Fprintf(f, "Started: %s\n\n", time.Now().Format(time.RFC3339))

	cmd.Stdout = io.MultiWriter(os.Stdout, f)
	cmd.Stderr = io.MultiWriter(os.Stderr, f)

	cmdErr := cmd.Run()

	if cmdErr != nil {
		fmt.Fprintf(f, "\n\n========== COMMAND FAILED: %v ==========\n", cmdErr)
	} else {
		fmt.Fprintf(f, "\n\n========== COMMAND SUCCEEDED ==========\n")
	}

	return cmdErr
}

