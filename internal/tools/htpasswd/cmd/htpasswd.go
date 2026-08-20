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
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/tools/htpasswd"
)

// htpasswdLong is a verbatim string rather than templates.LongDesc so the
// usage forms and the algorithm table keep their line breaks (LongDesc reflows
// text and would collapse them into paragraphs).
const htpasswdLong = `Manage password files and hash passwords, a self-contained analog of Apache htpasswd. No external htpasswd binary is required.

Usage mirrors htpasswd:

  d8 tools htpasswd [-cbdps... -C cost -r rounds] passwordfile username
  d8 tools htpasswd -b [...] passwordfile username password
  d8 tools htpasswd -n [-bmBdps...] [username]
  d8 tools htpasswd -D passwordfile username
  d8 tools htpasswd -v passwordfile username

Algorithms (choose one; default bcrypt):

  -B  bcrypt         secure, the default
  -m  apr1 MD5       Apache MD5, legacy htpasswd default
  -2  SHA-256 crypt  secure
  -5  SHA-512 crypt  secure
  -d  CRYPT (DES)    INSECURE, only first 8 chars used
  -s  SHA-1          INSECURE, unsalted
  -p  plaintext      INSECURE, no hashing

Unlike Apache htpasswd (apr1 at cost 5 by default), d8 defaults to bcrypt at cost 10, so 'd8 tools htpasswd -n <username>' produces a strong hash ready for 'd8 iam user create/reset-password --password-hash'. With -n and no username the bare hash is printed, which is exactly what --password-hash expects. The -2 and -5 SHA-crypt algorithms and the -r rounds flag are d8 extensions; Apache htpasswd has no -2, -5, or -r flag.

© Flant JSC 2026`

var htpasswdExample = templates.Examples(`
  # Create a file and add a user (prompts for the password)
  d8 tools htpasswd -c users.htpasswd alice

  # Add/update a user non-interactively (password from stdin)
  echo -n 'S3cret!' | d8 tools htpasswd -i users.htpasswd bob

  # Verify a password, then delete the user
  d8 tools htpasswd -bv users.htpasswd bob 'S3cret!'
  d8 tools htpasswd -D users.htpasswd bob

  # Print a bcrypt hash for 'd8 iam user ... --password-hash'
  HASH="$(echo -n 'Test12345!' | d8 tools htpasswd -ni)"
  d8 iam user reset-password test-user --password-hash "$HASH"

  # Other algorithms: apr1 line for an Ingress basic-auth secret; SHA-512 crypt
  d8 tools htpasswd -nbm admin 'S3cret!'
  d8 tools htpasswd -nb5 -r 100000 admin 'S3cret!'`)

func NewCommand() *cobra.Command {
	htpasswdCmd := &cobra.Command{
		Use:           "htpasswd [flags] [passwordfile] [username] [password]",
		Short:         "Manage password files and hash passwords (Apache htpasswd analog)",
		Long:          htpasswdLong,
		Example:       htpasswdExample,
		Args:          cobra.ArbitraryArgs,
		SilenceUsage:  true,
		SilenceErrors: false,
		RunE:          htpasswd.Htpasswd,
	}

	addFlags(htpasswdCmd.Flags())

	return htpasswdCmd
}
