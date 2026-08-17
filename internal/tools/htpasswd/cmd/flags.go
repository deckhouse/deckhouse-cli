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

package cmd

import (
	"github.com/spf13/pflag"

	"github.com/deckhouse/deckhouse-cli/internal/tools/htpasswd"
)

// addFlags wires up the Apache htpasswd flag surface. Long names are added for
// readability; the single-character shorthands match htpasswd exactly so
// existing muscle memory and scripts keep working (including bundling like
// -nbB and digit algorithm flags -2 / -5).
func addFlags(flags *pflag.FlagSet) {
	// Operation mode.
	flags.BoolP(htpasswd.FlagCreate, "c", false, "Create a new password file, overwriting any existing one.")
	flags.BoolP(htpasswd.FlagStdout, "n", false, "Do not update a file; print the result to stdout.")
	flags.BoolP(htpasswd.FlagDelete, "D", false, "Delete the given user from the password file.")
	flags.BoolP(htpasswd.FlagVerify, "v", false, "Verify the given password for the user.")

	// Password source.
	flags.BoolP(htpasswd.FlagBatch, "b", false, "Batch mode: take the password from the command line.")
	flags.BoolP(htpasswd.FlagStdin, "i", false, "Read the password from stdin without verification.")

	// Algorithm selection (default: bcrypt).
	flags.BoolP(htpasswd.FlagBcrypt, "B", false, "Use bcrypt (secure; the default).")
	flags.BoolP(htpasswd.FlagMD5, "m", false, "Use Apache MD5 (apr1).")
	flags.BoolP(htpasswd.FlagSHA256, "2", false, "Use SHA-256 crypt (secure).")
	flags.BoolP(htpasswd.FlagSHA512, "5", false, "Use SHA-512 crypt (secure).")
	flags.BoolP(htpasswd.FlagCrypt, "d", false, "Use CRYPT (DES; INSECURE, 8-char limit).")
	flags.BoolP(htpasswd.FlagSHA1, "s", false, "Use SHA-1 (INSECURE, unsalted).")
	flags.BoolP(htpasswd.FlagPlaintext, "p", false, "Store the password in plaintext (INSECURE).")

	// Algorithm parameters.
	flags.IntP(htpasswd.FlagCost, "C", htpasswd.DefaultBcryptCost, "bcrypt cost/work factor (4-31); only with -B.")
	flags.IntP(htpasswd.FlagRounds, "r", 0, "SHA-256/512 rounds (1000-999999999); only with -2/-5.")
}
