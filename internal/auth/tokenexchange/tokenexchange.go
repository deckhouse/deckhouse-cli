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

package tokenexchange

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
)

const (
	// OAuth 2.0 Token Exchange grant type (RFC 8693)
	GrantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"

	// Token types
	TokenTypeIDToken     = "urn:ietf:params:oauth:token-type:id_token"
	TokenTypeAccessToken = "urn:ietf:params:oauth:token-type:access_token"

	// Default scope for Kubernetes access
	DefaultScope = "openid profile email groups audience:server:client_id:kubernetes"

	// HTTP client timeout
	DefaultTimeout = 30 * time.Second
)

// Config holds the configuration for token exchange request.
type Config struct {
	// DexURL is the base URL of the Dex server (e.g., https://dex.example.com)
	DexURL string

	// ClientID is the OAuth2 client ID registered in Dex
	ClientID string

	// ClientSecret is the OAuth2 client secret
	ClientSecret string

	// SubjectToken is the token from the IdP to exchange
	SubjectToken string

	// SubjectTokenType is the type of the subject token (id_token or access_token)
	SubjectTokenType string

	// ConnectorID is the Dex connector identifier
	ConnectorID string

	// Scope is the OAuth2 scope for the requested token
	Scope string

	// RequestedTokenType is the type of token to request (id_token or access_token)
	RequestedTokenType string

	// InsecureSkipVerify skips TLS certificate verification
	InsecureSkipVerify bool

	// CAFile is the path to a CA certificate file for TLS verification
	CAFile string
}

// Response represents the token exchange response from Dex.
// See RFC 8693 Section 2.2.1: https://www.rfc-editor.org/rfc/rfc8693.html#section-2.2.1
type Response struct {
	AccessToken     string `json:"access_token"`
	IssuedTokenType string `json:"issued_token_type"`
	TokenType       string `json:"token_type"`
	ExpiresIn       int    `json:"expires_in"`
}

// ErrorResponse represents an OAuth2 error response.
// See RFC 6749 Section 5.2: https://www.rfc-editor.org/rfc/rfc6749#section-5.2
type ErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description,omitempty"`
}

// Client performs token exchange operations with Dex.
type Client struct {
	httpClient *http.Client
	config     *Config
}

// NewClient creates a new token exchange client with the given configuration.
func NewClient(cfg *Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	httpClient, err := createHTTPClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	return &Client{
		httpClient: httpClient,
		config:     cfg,
	}, nil
}

// Validate checks that all required configuration fields are set.
func (c *Config) Validate() error {
	var errs *multierror.Error

	if c.DexURL == "" {
		errs = multierror.Append(errs, fmt.Errorf("--dex-url is required"))
	}
	if c.ClientID == "" {
		errs = multierror.Append(errs, fmt.Errorf("--client-id is required"))
	}
	if c.ClientSecret == "" {
		errs = multierror.Append(errs, fmt.Errorf("--client-secret is required"))
	}
	if c.SubjectToken == "" {
		errs = multierror.Append(errs, fmt.Errorf("--subject-token is required"))
	}
	if c.ConnectorID == "" {
		errs = multierror.Append(errs, fmt.Errorf("--connector-id is required"))
	}

	if c.SubjectTokenType != "" && c.SubjectTokenType != "id_token" && c.SubjectTokenType != "access_token" {
		errs = multierror.Append(errs, fmt.Errorf("--subject-token-type must be 'id_token' or 'access_token'"))
	}

	if c.RequestedTokenType != "" && c.RequestedTokenType != "id_token" && c.RequestedTokenType != "access_token" {
		errs = multierror.Append(errs, fmt.Errorf("--requested-token-type must be 'id_token' or 'access_token'"))
	}

	return errs.ErrorOrNil()
}

// Exchange performs the token exchange and returns the response.
func (c *Client) Exchange() (*Response, error) {
	tokenURL, err := c.buildTokenURL()
	if err != nil {
		return nil, fmt.Errorf("failed to build token URL: %w", err)
	}

	formData := c.buildFormData()

	req, err := http.NewRequest(http.MethodPost, tokenURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(c.config.ClientID, c.config.ClientSecret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp.StatusCode, body)
	}

	var tokenResp Response
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &tokenResp, nil
}

// buildTokenURL constructs the Dex token endpoint URL.
func (c *Client) buildTokenURL() (string, error) {
	baseURL := strings.TrimSuffix(c.config.DexURL, "/")
	tokenURL, err := url.JoinPath(baseURL, "/token")
	if err != nil {
		return "", err
	}
	return tokenURL, nil
}

// buildFormData creates the form data for the token exchange request.
func (c *Client) buildFormData() url.Values {
	data := url.Values{}
	data.Set("grant_type", GrantTypeTokenExchange)
	data.Set("subject_token", c.config.SubjectToken)
	data.Set("connector_id", c.config.ConnectorID)

	// Set subject token type
	subjectTokenType := c.config.SubjectTokenType
	if subjectTokenType == "" {
		subjectTokenType = "id_token"
	}
	if subjectTokenType == "id_token" {
		data.Set("subject_token_type", TokenTypeIDToken)
	} else {
		data.Set("subject_token_type", TokenTypeAccessToken)
	}

	// Set scope
	scope := c.config.Scope
	if scope == "" {
		scope = DefaultScope
	}
	data.Set("scope", scope)

	// Set requested token type
	requestedTokenType := c.config.RequestedTokenType
	if requestedTokenType == "" {
		requestedTokenType = "id_token"
	}
	if requestedTokenType == "id_token" {
		data.Set("requested_token_type", TokenTypeIDToken)
	} else {
		data.Set("requested_token_type", TokenTypeAccessToken)
	}

	return data
}

// handleErrorResponse processes an error response from Dex with contextual hints.
func (c *Client) handleErrorResponse(statusCode int, body []byte) error {
	var errResp ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		return fmt.Errorf("token exchange failed with status %d: %s", statusCode, string(body))
	}

	baseMsg := errResp.Error
	if errResp.ErrorDescription != "" {
		baseMsg = fmt.Sprintf("%s - %s", errResp.Error, errResp.ErrorDescription)
	}

	hint := getErrorHint(statusCode, errResp.Error)
	if hint != "" {
		return fmt.Errorf("token exchange failed (status %d): %s\nHint: %s", statusCode, baseMsg, hint)
	}
	return fmt.Errorf("token exchange failed (status %d): %s", statusCode, baseMsg)
}

// getErrorHint returns a contextual hint based on the error status code and type.
func getErrorHint(statusCode int, errorType string) string {
	switch statusCode {
	case 400:
		switch errorType {
		case "invalid_request":
			return "Check that subject_token, subject_token_type, and connector_id are correct. " +
				"Ensure connector_id matches the DexProvider metadata.name."
		case "invalid_grant":
			return "The subject_token may be expired or malformed. " +
				"For Keycloak, ensure DexProvider has 'getUserInfo: true'."
		default:
			return "Verify subject_token, subject_token_type, and connector_id parameters."
		}
	case 401:
		switch errorType {
		case "invalid_client":
			return "Check client_id and client_secret. " +
				"client_id format should be 'dex-client-<name>@<namespace>'."
		case "access_denied":
			return "The subject_token is invalid or the IdP rejected it. " +
				"Verify the token is not expired and was issued for the correct audience."
		default:
			return "Verify client credentials and ensure the subject_token is valid."
		}
	case 403:
		return "The DexClient may be missing the annotation 'dexclient.deckhouse.io/allow-access-to-kubernetes: true'."
	default:
		return ""
	}
}

// createHTTPClient creates an HTTP client with the appropriate TLS configuration.
func createHTTPClient(cfg *Config) (*http.Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   DefaultTimeout,
	}, nil
}
