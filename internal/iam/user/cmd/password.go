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

	"github.com/spf13/cobra"
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
	// passwordModeHash means the caller provided a pre-computed bcrypt hash
	// directly via --password-hash; no plaintext is read.
	passwordModeHash
)

// passwordFlagSet is the flag layout used by both `user create` and
// `user reset-password`. Centralising the names keeps the two commands in
// lockstep so muscle memory transfers and shells autocomplete the same set.
const (
	flagPasswordPrompt   = "password-prompt"
	flagPasswordStdin    = "password-stdin"
	flagPasswordGenerate = "generate-password"
	flagPasswordHash     = "password-hash"
)

func addPasswordFlags(cmd *cobra.Command) {
	cmd.Flags().Bool(flagPasswordPrompt, false, "Read password interactively with hidden input")
	cmd.Flags().Bool(flagPasswordStdin, false, "Read password from stdin (for CI/pipelines)")
	cmd.Flags().Bool(flagPasswordGenerate, false, "Auto-generate a strong password (shown once on stderr)")
	cmd.Flags().String(flagPasswordHash, "", "Use a pre-computed bcrypt hash directly (must start with $2 and be a valid bcrypt cost)")
}

// resolvePasswordMode determines which password source to use based on the
// flags provided. Exactly one of the four sources may be set; if none is set,
// the mode is inferred from whether stdin is a terminal (prompt) or not (error).
func resolvePasswordMode(prompt, stdin, generate bool, hash string) (passwordMode, error) {
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
	if hash != "" {
		count++
	}
	if count > 1 {
		return passwordModeNone, fmt.Errorf(
			"only one of --%s, --%s, --%s, --%s may be specified",
			flagPasswordPrompt, flagPasswordStdin, flagPasswordGenerate, flagPasswordHash,
		)
	}

	switch {
	case hash != "":
		return passwordModeHash, nil
	case prompt:
		return passwordModePrompt, nil
	case stdin:
		return passwordModeStdin, nil
	case generate:
		return passwordModeGenerate, nil
	}

	if term.IsTerminal(int(os.Stdin.Fd())) {
		return passwordModePrompt, nil
	}
	return passwordModeNone, fmt.Errorf(
		"stdin is not a terminal; use --%s, --%s or --%s",
		flagPasswordStdin, flagPasswordGenerate, flagPasswordHash,
	)
}

// passwordResult is the raw result of a password input flow. Either Plain or
// Hash is set, never both. For passwordModeGenerate, Plain is set AND the
// caller is expected to display it once to the user (it is not retrievable
// from the bcrypt hash).
type passwordResult struct {
	Plain string // plaintext supplied by user / generated; empty for hash mode
	Hash  string // raw bcrypt hash ($2y$... or $2a$...); set when input was a hash
}

// resolvePasswordInput executes the configured password-input flow and returns
// the user-supplied plaintext or the validated bcrypt hash. Caller decides how
// to apply the result (User.spec.password expects base64(bcrypt); UserOperation
// expects raw bcrypt — see User CR doc and user-authn hooks).
func resolvePasswordInput(cmd *cobra.Command) (passwordResult, passwordMode, error) {
	prompt, _ := cmd.Flags().GetBool(flagPasswordPrompt)
	stdinFlag, _ := cmd.Flags().GetBool(flagPasswordStdin)
	generate, _ := cmd.Flags().GetBool(flagPasswordGenerate)
	hashFlag, _ := cmd.Flags().GetString(flagPasswordHash)

	mode, err := resolvePasswordMode(prompt, stdinFlag, generate, hashFlag)
	if err != nil {
		return passwordResult{}, mode, err
	}

	switch mode {
	case passwordModeHash:
		if err := validateBcryptHash(hashFlag); err != nil {
			return passwordResult{}, mode, err
		}
		return passwordResult{Hash: hashFlag}, mode, nil
	case passwordModePrompt:
		p, err := readPasswordPrompt(int(os.Stdin.Fd()), cmd.ErrOrStderr())
		return passwordResult{Plain: p}, mode, err
	case passwordModeStdin:
		p, err := readPasswordStdin(os.Stdin)
		return passwordResult{Plain: p}, mode, err
	case passwordModeGenerate:
		p, err := generatePassword()
		return passwordResult{Plain: p}, mode, err
	}
	return passwordResult{}, mode, errors.New("no password source configured")
}

// rawBcryptHash returns the raw bcrypt hash string ($2y$... or $2a$...). For
// hash-mode inputs the hash was already validated; for plaintext inputs we
// hash here once.
func (r passwordResult) rawBcryptHash() (string, error) {
	if r.Hash != "" {
		return r.Hash, nil
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(r.Plain), bcryptCost)
	if err != nil {
		return "", fmt.Errorf("hashing password: %w", err)
	}
	return string(hash), nil
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

// encodePasswordForUserCR returns the value to write into User.spec.password.
// The User CR stores base64(<raw bcrypt hash>); the user-authn hook reads
// it via getDexUserCRDs and forwards the bytes to Dex Password.hash. For raw
// bcrypt input we still need the base64 wrap because we cannot tell at write
// time whether the controller is at a version that auto-detects.
func encodePasswordForUserCR(rawHash string) string {
	return base64.StdEncoding.EncodeToString([]byte(rawHash))
}

// encodePasswordForDeckhouse is kept as a thin wrapper for backward-compatibility
// with existing tests; it hashes plaintext and returns the User-CR encoding.
func encodePasswordForDeckhouse(plain string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcryptCost)
	if err != nil {
		return "", fmt.Errorf("hashing password: %w", err)
	}
	return encodePasswordForUserCR(string(hash)), nil
}

// validateBcryptHash checks that the supplied string is a syntactically valid
// bcrypt hash (the prefix and the bcrypt.Cost call). We do not verify against
// a known plaintext (we don't have one); a malformed hash would only be caught
// by the user-authn hook later, well after the apiserver accepted the CR.
func validateBcryptHash(s string) error {
	if !strings.HasPrefix(s, "$2") {
		return fmt.Errorf("--%s value does not look like a bcrypt hash (expected to start with $2a$ or $2y$)", flagPasswordHash)
	}
	if _, err := bcrypt.Cost([]byte(s)); err != nil {
		return fmt.Errorf("--%s value is not a valid bcrypt hash: %w", flagPasswordHash, err)
	}
	return nil
}
