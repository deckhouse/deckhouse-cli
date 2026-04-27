/*
Copyright 2026 Flant JSC

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

package user

import (
	"bufio"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/term"
)

const bcryptCost = 10

type passwordMode int

const (
	passwordModeNone passwordMode = iota
	passwordModePrompt
	passwordModeStdin
	passwordModeGenerate
)

// resolvePasswordMode determines which password source to use based on the flags provided.
// Exactly one of prompt, stdin, or generate must be true; if none is set, the mode is
// inferred from whether stdin is a terminal.
func resolvePasswordMode(prompt, stdin, generate bool) (passwordMode, error) {
	count := 0
	if prompt {
		count++
	}
	if stdin {
		count++
	}
	if generate {
		count++
	}
	if count > 1 {
		return passwordModeNone, errors.New("only one of --password-prompt, --password-stdin, --generate-password may be specified")
	}

	if prompt {
		return passwordModePrompt, nil
	}
	if stdin {
		return passwordModeStdin, nil
	}
	if generate {
		return passwordModeGenerate, nil
	}

	if term.IsTerminal(int(os.Stdin.Fd())) {
		return passwordModePrompt, nil
	}
	return passwordModeNone, errors.New("stdin is not a terminal; use --password-stdin or --generate-password")
}

// readPasswordPrompt reads a password interactively with confirmation.
// stdinFd is the file descriptor to read from; out receives prompts.
func readPasswordPrompt(stdinFd int, out io.Writer) (string, error) {
	fmt.Fprint(out, "Enter password: ")
	pw1, err := term.ReadPassword(stdinFd)
	fmt.Fprintln(out)
	if err != nil {
		return "", fmt.Errorf("reading password: %w", err)
	}

	fmt.Fprint(out, "Confirm password: ")
	pw2, err := term.ReadPassword(stdinFd)
	fmt.Fprintln(out)
	if err != nil {
		return "", fmt.Errorf("reading password confirmation: %w", err)
	}

	if string(pw1) != string(pw2) {
		return "", errors.New("passwords do not match")
	}
	if len(pw1) == 0 {
		return "", errors.New("password must not be empty")
	}
	return string(pw1), nil
}

// readPasswordStdin reads a single line from the given reader.
func readPasswordStdin(r io.Reader) (string, error) {
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("reading password from stdin: %w", err)
		}
		return "", errors.New("no password provided on stdin")
	}
	pw := strings.TrimRight(scanner.Text(), "\r\n")
	if pw == "" {
		return "", errors.New("password must not be empty")
	}
	return pw, nil
}

// generatePassword creates a cryptographically random password.
func generatePassword() (string, error) {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating random password: %w", err)
	}
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b), nil
}

// encodePasswordForDeckhouse hashes the plaintext password with bcrypt cost 10
// and returns a base64-encoded bcrypt hash suitable for User.spec.password.
func encodePasswordForDeckhouse(plain string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcryptCost)
	if err != nil {
		return "", fmt.Errorf("hashing password: %w", err)
	}
	return base64.StdEncoding.EncodeToString(hash), nil
}
