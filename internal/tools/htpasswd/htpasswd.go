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

package htpasswd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/term"
)

// DefaultBcryptCost is the bcrypt work factor used when -C is not given. It
// deliberately differs from Apache htpasswd (whose default is cost 5, and whose
// default algorithm is apr1-MD5): d8 defaults to bcrypt at cost 10 so the hash
// is strong and directly usable by 'd8 iam user ... --password-hash'. Every
// individual algorithm flag still behaves exactly like htpasswd.
const DefaultBcryptCost = 10

// Flag names. Each maps to an Apache htpasswd short flag in flags.go.
const (
	FlagCreate    = "create"    // -c
	FlagStdout    = "stdout"    // -n
	FlagBatch     = "batch"     // -b
	FlagStdin     = "stdin"     // -i
	FlagMD5       = "md5"       // -m
	FlagSHA256    = "sha256"    // -2
	FlagSHA512    = "sha512"    // -5
	FlagBcrypt    = "bcrypt"    // -B
	FlagCrypt     = "des"       // -d
	FlagSHA1      = "sha1"      // -s
	FlagPlaintext = "plaintext" // -p
	FlagDelete    = "delete"    // -D
	FlagVerify    = "verify"    // -v
	FlagCost      = "cost"      // -C
	FlagRounds    = "rounds"    // -r
)

// options is the resolved view of the command-line flags.
type options struct {
	create  bool
	stdout  bool
	batch   bool
	stdinPw bool
	delete  bool
	verify  bool

	cost      int
	costSet   bool
	rounds    int
	roundsSet bool

	alg algorithm
}

// Htpasswd is the cobra RunE entry point: it resolves flags, validates the
// combination, and dispatches to the stdout or file-management flow.
func Htpasswd(cmd *cobra.Command, args []string) error {
	o, err := parseOptions(cmd)
	if err != nil {
		return err
	}

	if err := o.validate(); err != nil {
		return err
	}

	if o.stdout {
		return runStdout(cmd, o, args)
	}

	if o.delete {
		return runDelete(cmd, o, args)
	}

	if o.verify {
		return runVerify(cmd, o, args)
	}

	return runAddUpdate(cmd, o, args)
}

func parseOptions(cmd *cobra.Command) (*options, error) {
	f := cmd.Flags()

	o := &options{}
	o.create, _ = f.GetBool(FlagCreate)
	o.stdout, _ = f.GetBool(FlagStdout)
	o.batch, _ = f.GetBool(FlagBatch)
	o.stdinPw, _ = f.GetBool(FlagStdin)
	o.delete, _ = f.GetBool(FlagDelete)
	o.verify, _ = f.GetBool(FlagVerify)
	o.cost, _ = f.GetInt(FlagCost)
	o.rounds, _ = f.GetInt(FlagRounds)
	o.costSet = f.Changed(FlagCost)
	o.roundsSet = f.Changed(FlagRounds)

	selected := []struct {
		flag string
		alg  algorithm
	}{
		{FlagBcrypt, algBcrypt},
		{FlagMD5, algAPR1},
		{FlagSHA256, algSHA256},
		{FlagSHA512, algSHA512},
		{FlagCrypt, algDES},
		{FlagSHA1, algSHA1},
		{FlagPlaintext, algPlain},
	}

	o.alg = algBcrypt

	found := false

	for _, s := range selected {
		if on, _ := f.GetBool(s.flag); on {
			if found {
				return nil, errors.New("only one hashing algorithm flag may be given (-B, -m, -2, -5, -d, -s, -p)")
			}

			o.alg = s.alg
			found = true
		}
	}

	return o, nil
}

func (o *options) validate() error {
	switch {
	case o.create && o.stdout:
		return errors.New("-c (create) and -n (stdout) cannot be combined")
	case o.stdout && o.delete:
		return errors.New("-n (stdout) and -D (delete) cannot be combined")
	case o.stdout && o.verify:
		return errors.New("-n (stdout) and -v (verify) cannot be combined")
	case o.verify && o.create:
		return errors.New("-v (verify) and -c (create) cannot be combined")
	case o.verify && o.delete:
		return errors.New("-v (verify) and -D (delete) cannot be combined")
	case o.create && o.delete:
		return errors.New("-c (create) and -D (delete) cannot be combined")
	case o.batch && o.stdinPw:
		return errors.New("-b (batch) and -i (stdin) cannot be combined")
	case o.delete && (o.batch || o.stdinPw):
		return errors.New("-D (delete) does not take a password")
	}

	if o.costSet && o.alg != algBcrypt {
		return errors.New("-C (cost) is only valid with -B (bcrypt)")
	}

	if o.roundsSet && o.alg != algSHA256 && o.alg != algSHA512 {
		return errors.New("-r (rounds) is only valid with -2 or -5")
	}

	if o.alg == algBcrypt && (o.cost < bcrypt.MinCost || o.cost > bcrypt.MaxCost) {
		return fmt.Errorf("bcrypt cost must be between %d and %d, got %d", bcrypt.MinCost, bcrypt.MaxCost, o.cost)
	}

	if o.roundsSet && (o.rounds < shaCryptMinRounds || o.rounds > shaCryptMaxRounds) {
		return fmt.Errorf("rounds must be between %d and %d, got %d", shaCryptMinRounds, shaCryptMaxRounds, o.rounds)
	}

	return nil
}

func (o *options) hashOptions() hashOptions {
	return hashOptions{alg: o.alg, cost: o.cost, rounds: o.rounds}
}

// runStdout implements '-n': hash a password and print it, never touching a
// file. When a username argument is present it prints the htpasswd
// "username:hash" line, so an explicit empty username ("") prints ":hash"
// exactly like Apache htpasswd — this makes 'd8 tools htpasswd -BinC 10 ""' a
// drop-in for 'htpasswd -BinC 10 ""'. When no username argument is given at all
// (an extension htpasswd lacks) it prints just the bare hash, which is what
// 'd8 iam user ... --password-hash' consumes.
func runStdout(cmd *cobra.Command, o *options, args []string) error {
	var username, password string

	hasUsername := false
	havePassword := false

	switch {
	case o.batch:
		switch len(args) {
		case 1:
			password = args[0]
		case 2:
			username, password, hasUsername = args[0], args[1], true
		default:
			return errors.New("usage: -n -b [username] <password>")
		}

		havePassword = true
	case len(args) > 1:
		return errors.New("usage: -n [username]")
	case len(args) == 1:
		username, hasUsername = args[0], true
	}

	if err := validateUsername(username); err != nil {
		return err
	}

	if !havePassword {
		p, err := readPassword(cmd, o, true)
		if err != nil {
			return err
		}

		password = p
	}

	hash, err := generateHash(password, o.hashOptions())
	if err != nil {
		return err
	}

	line := hash
	if hasUsername {
		line = username + ":" + hash
	}

	_, err = fmt.Fprintln(cmd.OutOrStdout(), line)

	return err
}

// runAddUpdate adds or replaces a user's entry in a password file.
func runAddUpdate(cmd *cobra.Command, o *options, args []string) error {
	file, username, password, err := fileUserPassword(cmd, o, args, true)
	if err != nil {
		return err
	}

	var pf *passwdFile

	if o.create {
		pf = newPasswdFile(file)
	} else {
		pf, err = loadPasswdFile(file)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("password file %q does not exist; pass -c to create it", file)
			}

			return err
		}
	}

	hash, err := generateHash(password, o.hashOptions())
	if err != nil {
		return err
	}

	existed := pf.upsert(username, hash)

	if err := pf.save(); err != nil {
		return err
	}

	action := "Adding"
	if existed {
		action = "Updating"
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "%s password for user %s\n", action, username)

	return nil
}

// runDelete removes a user from a password file.
func runDelete(cmd *cobra.Command, _ *options, args []string) error {
	if len(args) != 2 {
		return errors.New("usage: -D <passwordfile> <username>")
	}

	file, username := args[0], args[1]

	pf, err := loadPasswdFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("password file %q does not exist", file)
		}

		return err
	}

	if !pf.remove(username) {
		return fmt.Errorf("user %s not found in %q", username, file)
	}

	if err := pf.save(); err != nil {
		return err
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "Deleting password for user %s\n", username)

	return nil
}

// runVerify checks a password against the stored hash for a user.
func runVerify(cmd *cobra.Command, o *options, args []string) error {
	file, username, password, err := fileUserPassword(cmd, o, args, false)
	if err != nil {
		return err
	}

	pf, err := loadPasswdFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("password file %q does not exist", file)
		}

		return err
	}

	stored, ok := pf.get(username)
	if !ok {
		return fmt.Errorf("user %s not found in %q", username, file)
	}

	match, err := verifyHash(password, stored)
	if err != nil {
		return err
	}

	if !match {
		return fmt.Errorf("password verification failed for user %s", username)
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "Password for user %s correct.\n", username)

	return nil
}

// fileUserPassword parses the "<passwordfile> <username> [password]" positional
// form shared by add/update and verify, and obtains the password from -b, -i,
// or an interactive prompt. confirm controls whether an interactive prompt asks
// for the password twice (true for setting a password, false for verifying).
func fileUserPassword(cmd *cobra.Command, o *options, args []string, confirm bool) (string, string, string, error) {
	var file, username, password string

	if o.batch {
		if len(args) != 3 {
			return "", "", "", errors.New("usage: -b <passwordfile> <username> <password>")
		}

		file, username, password = args[0], args[1], args[2]
	} else {
		if len(args) != 2 {
			return "", "", "", errors.New("usage: <passwordfile> <username>")
		}

		file, username = args[0], args[1]

		p, err := readPassword(cmd, o, confirm)
		if err != nil {
			return "", "", "", err
		}

		password = p
	}

	if err := validateUsername(username); err != nil {
		return "", "", "", err
	}

	return file, username, password, nil
}

// validateUsername enforces the htpasswd constraints: no colon (the field
// separator), no control characters, and at most 255 bytes.
func validateUsername(username string) error {
	if strings.Contains(username, ":") {
		return errors.New("username must not contain a ':' character")
	}

	if strings.ContainsAny(username, "\n\r\t") {
		return errors.New("username must not contain control characters")
	}

	if len(username) > 255 {
		return errors.New("username must be at most 255 characters")
	}

	return nil
}

// readPassword obtains the password from stdin (with -i, or whenever stdin is
// not a terminal) or from an interactive hidden prompt. An interactive prompt
// asks twice and checks the two entries match when confirm is set.
func readPassword(cmd *cobra.Command, o *options, confirm bool) (string, error) {
	if o.stdinPw || !term.IsTerminal(int(os.Stdin.Fd())) {
		return readPasswordLine(os.Stdin)
	}

	return readPasswordPrompt(cmd.ErrOrStderr(), int(os.Stdin.Fd()), confirm)
}

// readPasswordLine reads and returns a single line, stripping the trailing
// newline so both `printf pw` and `echo pw` yield the same password.
func readPasswordLine(r io.Reader) (string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("reading password from stdin: %w", err)
		}

		return "", errors.New("no password provided on stdin")
	}

	password := strings.TrimRight(scanner.Text(), "\r\n")
	if password == "" {
		return "", errors.New("password must not be empty")
	}

	return password, nil
}

// readPasswordPrompt reads a hidden password from the terminal, optionally
// asking a second time and checking the two match.
func readPasswordPrompt(prompts io.Writer, stdinFd int, confirm bool) (string, error) {
	fmt.Fprint(prompts, "New password: ")

	first, err := term.ReadPassword(stdinFd)

	fmt.Fprintln(prompts)

	if err != nil {
		return "", fmt.Errorf("reading password: %w", err)
	}

	if len(first) == 0 {
		return "", errors.New("password must not be empty")
	}

	if !confirm {
		return string(first), nil
	}

	fmt.Fprint(prompts, "Re-type new password: ")

	second, err := term.ReadPassword(stdinFd)

	fmt.Fprintln(prompts)

	if err != nil {
		return "", fmt.Errorf("reading password confirmation: %w", err)
	}

	if string(first) != string(second) {
		return "", errors.New("passwords do not match")
	}

	return string(first), nil
}
