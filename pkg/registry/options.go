package registry

import (
	"crypto/tls"
	"log/slog"
	"net/http"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse/pkg/log"
)

// ClientOptions contains configuration options for the registry client
type ClientOptions struct {
	// Registry is the registry host (e.g., "registry.example.com")
	Registry string

	// Username for basic authentication
	Username string

	// Password for basic authentication
	Password string

	// LicenseToken for Deckhouse license authentication (alternative to Username/Password)
	LicenseToken string

	// Insecure allows connecting to registries over HTTP instead of HTTPS
	Insecure bool

	// TLSSkipVerify skips TLS certificate verification
	TLSSkipVerify bool

	// Logger for client operations
	Logger *log.Logger
}

// ensureLogger sets a default logger if none is provided
func ensureLogger(opts *ClientOptions) {
	if opts.Logger == nil {
		opts.Logger = log.NewLogger().Named("registry-client")
	}
}

// buildAuthenticator creates the appropriate authenticator based on provided credentials
// Priority: License Token > Username/Password > Anonymous
func buildAuthenticator(opts *ClientOptions) authn.Authenticator {
	ensureLogger(opts)

	if opts.LicenseToken != "" {
		opts.Logger.Debug("Registry client initialized with license token authentication",
			slog.String("registry", opts.Registry))
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: opts.LicenseToken,
		})
	}

	if opts.Username != "" && opts.Password != "" {
		opts.Logger.Debug("Registry client initialized with basic authentication",
			slog.String("registry", opts.Registry),
			slog.String("username", opts.Username))
		return authn.FromConfig(authn.AuthConfig{
			Username: opts.Username,
			Password: opts.Password,
		})
	}

	opts.Logger.Debug("Registry client initialized with anonymous access",
		slog.String("registry", opts.Registry))
	return authn.Anonymous
}

// buildRemoteOptions constructs remote options including auth and transport configuration
func buildRemoteOptions(auth authn.Authenticator, opts *ClientOptions) []remote.Option {
	remoteOptions := []remote.Option{
		remote.WithAuth(auth),
	}

	if needsCustomTransport(opts) {
		transport := configureTransport(opts)
		remoteOptions = append(remoteOptions, remote.WithTransport(transport))
	}

	return remoteOptions
}

// needsCustomTransport checks if custom transport configuration is required
func needsCustomTransport(opts *ClientOptions) bool {
	return opts.Insecure || opts.TLSSkipVerify
}

// configureTransport creates and configures an HTTP transport with TLS settings
func configureTransport(opts *ClientOptions) *http.Transport {
	transport := remote.DefaultTransport.(*http.Transport).Clone()

	if opts.TLSSkipVerify {
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		}
		transport.TLSClientConfig.InsecureSkipVerify = true
		opts.Logger.Debug("TLS certificate verification disabled",
			slog.String("registry", opts.Registry))
	}

	if opts.Insecure {
		opts.Logger.Debug("Insecure HTTP mode enabled",
			slog.String("registry", opts.Registry))
	}

	return transport
}
