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

package basic

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/rootflagnames"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

const loginLong = `Log in to a container registry.

Verifies the credentials against the registry and, on success, writes them to
the Docker config (~/.docker/config.json, honouring $DOCKER_CONFIG). Every
other "d8 cr" command then authenticates from that config automatically, the
same way "docker login" works.

Credentials come from the --username/--password flags; either may be omitted
to be prompted interactively (the password is read without echo). With no
REGISTRY argument the default Docker Hub registry is used.`

const loginExample = `  # Prompt for username and password
  d8 cr login registry.example.com

  # Non-interactive (e.g. CI)
  d8 cr login registry.example.com --username robot --password "$TOKEN"

  # Log in to Docker Hub
  d8 cr login --username alice --password "$TOKEN"`

func NewLoginCmd(opts *registry.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "login [REGISTRY]",
		Short:             "Log in to a container registry",
		Long:              loginLong,
		Example:           loginExample,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completion.RegistryHost(),
		RunE: func(cmd *cobra.Command, args []string) error {
			var host string
			if len(args) == 1 {
				host = args[0]
			}

			username, _ := cmd.Flags().GetString(rootflagnames.Username)
			password, _ := cmd.Flags().GetString(rootflagnames.Password)

			username, password, err := resolveCredentials(cmd.InOrStdin(), cmd.OutOrStdout(), username, password)
			if err != nil {
				return err
			}

			res, err := registry.Login(cmd.Context(), host, username, password, opts)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Login succeeded for %s\nCredentials saved in %s\n", res.ServerAddress, res.ConfigFile)

			return nil
		},
	}

	return cmd
}

// resolveCredentials fills in any missing username/password by prompting on
// the terminal. The password prompt suppresses echo when stdin is a TTY;
// otherwise (piped input) it reads a plain line so scripts still work.
func resolveCredentials(in io.Reader, out io.Writer, username, password string) (string, string, error) {
	reader := bufio.NewReader(in)

	if username == "" {
		fmt.Fprint(out, "Username: ")

		line, err := reader.ReadString('\n')
		if err != nil && line == "" {
			return "", "", fmt.Errorf("read username: %w", err)
		}

		username = strings.TrimSpace(line)
		if username == "" {
			return "", "", fmt.Errorf("username is required")
		}
	}

	if password == "" {
		fmt.Fprint(out, "Password: ")

		if f, ok := in.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
			b, err := term.ReadPassword(int(f.Fd()))

			fmt.Fprintln(out)

			if err != nil {
				return "", "", fmt.Errorf("read password: %w", err)
			}

			password = string(b)
		} else {
			line, err := reader.ReadString('\n')
			if err != nil && line == "" {
				return "", "", fmt.Errorf("read password: %w", err)
			}

			password = strings.TrimRight(line, "\r\n")
		}

		if password == "" {
			return "", "", fmt.Errorf("password is required")
		}
	}

	return username, password, nil
}
