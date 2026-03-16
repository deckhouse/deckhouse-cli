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
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/auth/tokenexchange"
)

var (
	tokenExchangeLong = templates.LongDesc(`
		Exchange an IdP token for a Dex token with Kubernetes audience.

		This command performs OAuth 2.0 Token Exchange (RFC 8693) with Dex,
		exchanging a token from an Identity Provider (IdP) for a Dex token
		that can be used to authenticate to Kubernetes API.

		This is useful for CI/CD pipelines where the pipeline obtains a token
		from an IdP (e.g., GitLab CI OIDC token, GitHub Actions OIDC token,
		or Keycloak access token) and needs to exchange it for a token
		that Kubernetes API server will accept.

		PREREQUISITES:
		- DexProvider must be configured as OIDC type
		- DexClient must have annotation: dexclient.deckhouse.io/allow-access-to-kubernetes: "true"
		- publishAPI must be enabled`)

	tokenExchangeExample = templates.Examples(`
		# Exchange GitLab CI OIDC token for Kubernetes token
		d8 login token-exchange \
		  --dex-url https://dex.example.com \
		  --client-id dex-client-ci-token-exchange@d8-user-authn \
		  --client-secret $DEX_CLIENT_SECRET \
		  --subject-token $GITLAB_OIDC_TOKEN \
		  --connector-id gitlab

		# Exchange Keycloak access token (use access_token type)
		d8 login token-exchange \
		  --dex-url https://dex.example.com \
		  --client-id dex-client-ci-token-exchange@d8-user-authn \
		  --client-secret $DEX_CLIENT_SECRET \
		  --subject-token $KEYCLOAK_TOKEN \
		  --subject-token-type access_token \
		  --connector-id keycloak

		# Get token and use it with kubectl
		export DEX_TOKEN=$(d8 login token-exchange \
		  --dex-url https://dex.example.com \
		  --client-id $DEX_CLIENT_ID \
		  --client-secret $DEX_CLIENT_SECRET \
		  --subject-token $IDP_TOKEN \
		  --connector-id gitlab)
		kubectl --server=$K8S_SERVER --token=$DEX_TOKEN get pods

		# Get full JSON response
		d8 login token-exchange \
		  --dex-url https://dex.example.com \
		  --client-id $DEX_CLIENT_ID \
		  --client-secret $DEX_CLIENT_SECRET \
		  --subject-token $IDP_TOKEN \
		  --connector-id gitlab \
		  --output json

		# Use environment variables for sensitive data
		export DEX_URL=https://dex.example.com
		export DEX_CLIENT_ID=dex-client-ci@d8-user-authn
		export DEX_CLIENT_SECRET=your-secret
		export DEX_CONNECTOR_ID=gitlab
		export SUBJECT_TOKEN=$GITLAB_OIDC_TOKEN
		d8 login token-exchange`)
)

// Flags for the token-exchange command
type tokenExchangeFlags struct {
	dexURL             string
	clientID           string
	clientSecret       string
	subjectToken       string
	subjectTokenType   string
	connectorID        string
	scope              string
	requestedTokenType string
	insecureSkipVerify bool
	caFile             string
	output             string
}

// NewCommand creates a new token-exchange command.
func NewCommand() *cobra.Command {
	flags := &tokenExchangeFlags{}

	cmd := &cobra.Command{
		Use:           "token-exchange",
		Short:         "Exchange an IdP token for a Dex token",
		Long:          tokenExchangeLong,
		Example:       tokenExchangeExample,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTokenExchange(flags)
		},
	}

	addFlags(cmd, flags)

	return cmd
}

func addFlags(cmd *cobra.Command, flags *tokenExchangeFlags) {
	f := cmd.Flags()

	f.StringVar(&flags.dexURL, "dex-url", os.Getenv("DEX_URL"),
		"URL of the Dex server (env: DEX_URL)")
	f.StringVar(&flags.clientID, "client-id", os.Getenv("DEX_CLIENT_ID"),
		"OAuth2 client ID (env: DEX_CLIENT_ID)")
	f.StringVar(&flags.clientSecret, "client-secret", os.Getenv("DEX_CLIENT_SECRET"),
		"OAuth2 client secret (env: DEX_CLIENT_SECRET)")
	f.StringVar(&flags.subjectToken, "subject-token", os.Getenv("SUBJECT_TOKEN"),
		"Token from IdP to exchange (env: SUBJECT_TOKEN)")
	f.StringVar(&flags.subjectTokenType, "subject-token-type", "id_token",
		"Type of subject token: 'id_token' (GitLab/GitHub) or 'access_token' (Keycloak)")
	f.StringVar(&flags.connectorID, "connector-id", os.Getenv("DEX_CONNECTOR_ID"),
		"Dex connector ID (env: DEX_CONNECTOR_ID)")
	f.StringVar(&flags.scope, "scope", tokenexchange.DefaultScope,
		"OAuth2 scope for the requested token")
	f.StringVar(&flags.requestedTokenType, "requested-token-type", "id_token",
		"Type of token to request: 'id_token' or 'access_token'")
	f.BoolVar(&flags.insecureSkipVerify, "tls-skip-verify", false,
		"Disable TLS certificate verification")
	f.StringVar(&flags.caFile, "ca-file", "",
		"Path to CA certificate file for TLS verification")
	f.StringVar(&flags.output, "output", "token",
		"Output format: 'token' (just the token) or 'json' (full response)")
}

func runTokenExchange(flags *tokenExchangeFlags) error {
	cfg := &tokenexchange.Config{
		DexURL:             flags.dexURL,
		ClientID:           flags.clientID,
		ClientSecret:       flags.clientSecret,
		SubjectToken:       flags.subjectToken,
		SubjectTokenType:   flags.subjectTokenType,
		ConnectorID:        flags.connectorID,
		Scope:              flags.scope,
		RequestedTokenType: flags.requestedTokenType,
		InsecureSkipVerify: flags.insecureSkipVerify,
		CAFile:             flags.caFile,
	}

	client, err := tokenexchange.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create token exchange client: %w", err)
	}

	resp, err := client.Exchange()
	if err != nil {
		return err
	}

	switch flags.output {
	case "json":
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(resp); err != nil {
			return fmt.Errorf("failed to encode response: %w", err)
		}
	case "token":
		fmt.Print(resp.AccessToken)
	default:
		return fmt.Errorf("unknown output format: %s", flags.output)
	}

	return nil
}
