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

package push

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
	"github.com/deckhouse/deckhouse-cli/internal/version"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/validation"
)

// CLI Parameters
var (
	TempDir string

	RegistryHost      string
	RegistryPath      string
	RegistryUsername  string
	RegistryPassword  string
	ModulesPathSuffix string

	Insecure         bool
	TLSSkipVerify    bool
	ImagesBundlePath string
)

const pushLong = `Upload Deckhouse Kubernetes Platform distribution bundle to the third-party registry.

This command pushes the Deckhouse Kubernetes Platform distribution into the specified container registry.

For more information on how to use it, consult the docs at 
https://deckhouse.io/products/kubernetes-platform/documentation/latest/installing/#manual-loading-of-dkp-images-and-vulnerability-db-into-a-private-registry

Additional configuration options for the d8 mirror family of commands are available as environment variables:

 * $SSL_CERT_FILE           — Path to the SSL certificate. If the variable is set, system certificates are not used;
 * $SSL_CERT_DIR            — List of directories to search for SSL certificate files, separated by a colon.
                              If set, system certificates are not used. More info at https://docs.openssl.org/1.0.2/man1/c_rehash/;
 * $HTTP_PROXY/$HTTPS_PROXY — URL of the proxy server for HTTP(S) requests to hosts that are not listed in the $NO_PROXY;
 * $NO_PROXY                — Comma-separated list of hosts to exclude from proxying.
                              Supported value formats include IP's', CIDR notations (1.2.3.4/8), domains, and asterisk sign (*).
                              The IP addresses and domain names can also include a literal port number (1.2.3.4:80).
                              The domain name matches that name and all the subdomains.
                              The domain name with a leading . matches subdomains only.
                              For example, foo.com matches foo.com and bar.foo.com; .y.com matches x.y.com but does not match y.com.
                              A single asterisk * indicates that no proxying should be done;

LICENSE NOTE:
The d8 mirror functionality is exclusively available to users holding a 
valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2025`

func NewCommand() *cobra.Command {
	pushCmd := &cobra.Command{
		Use:           "push <images-bundle-path> <registry>",
		Short:         "Copy Deckhouse Kubernetes Platform distribution to the third-party registry",
		Long:          pushLong,
		ValidArgs:     []string{"images-bundle-path", "registry"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       parseAndValidateParameters,
		RunE: func(_ *cobra.Command, _ []string) error {
			return NewPusher().Execute()
		},
		PostRunE: func(_ *cobra.Command, _ []string) error {
			return os.RemoveAll(TempDir)
		},
	}

	addFlags(pushCmd.Flags())
	return pushCmd
}

func setupLogger() *log.SLogger {
	logLevel := slog.LevelInfo
	if log.DebugLogLevel() >= 3 {
		logLevel = slog.LevelDebug
	}
	return log.NewSLogger(logLevel)
}

func buildPushParams(logger params.Logger) *params.PushParams {
	pushParams := &params.PushParams{
		BaseParams: params.BaseParams{
			Logger:              logger,
			Insecure:            Insecure,
			SkipTLSVerification: TLSSkipVerify,
			RegistryHost:        RegistryHost,
			RegistryPath:        RegistryPath,
			ModulesPathSuffix:   ModulesPathSuffix,
			BundleDir:           ImagesBundlePath,
			WorkingDir:          filepath.Join(TempDir, "push"),
		},

		Parallelism: params.ParallelismConfig{
			Blobs:  4,
			Images: 1,
		},
	}
	return pushParams
}

func validateRegistryAccess(ctx context.Context, pushParams *params.PushParams) error {
	opts := []validation.Option{
		validation.UseAuthProvider(pushParams.RegistryAuth),
		validation.WithInsecure(pushParams.Insecure),
		validation.WithTLSVerificationSkip(pushParams.SkipTLSVerification),
	}

	accessValidator := validation.NewRemoteRegistryAccessValidator()
	err := accessValidator.ValidateWriteAccessForRepo(ctx, path.Join(pushParams.RegistryHost, pushParams.RegistryPath), opts...)
	if err != nil {
		return fmt.Errorf("validate write access to registry %s: %w", path.Join(pushParams.RegistryHost, pushParams.RegistryPath), err)
	}

	return nil
}

// Pusher handles the push operation for Deckhouse distribution
type Pusher struct {
	logger     params.Logger
	pushParams *params.PushParams
}

// NewPusher creates a new Pusher instance
func NewPusher() *Pusher {
	logger := setupLogger()
	pushParams := buildPushParams(logger)
	return &Pusher{
		logger:     logger,
		pushParams: pushParams,
	}
}

// Execute runs the full push process
func (p *Pusher) Execute() error {
	p.logger.Infof("d8 version: %s", version.Version)

	if RegistryUsername != "" {
		p.pushParams.RegistryAuth = authn.FromConfig(authn.AuthConfig{Username: RegistryUsername, Password: RegistryPassword})
	}

	if err := p.validateRegistryAccess(); err != nil {
		return err
	}

	return p.executeNewPush()
}

// executeNewPush runs the push using the push service.
// This service expects the bundle to have the exact same structure as the registry:
// - Each OCI layout's relative path becomes its registry segment
// - Works with unified bundles where pull saved the structure as-is
func (p *Pusher) executeNewPush() error {
	// Set up graceful cancellation on Ctrl+C
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := dkplog.NewNop()

	if log.DebugLogLevel() >= 3 {
		logger = dkplog.NewLogger(dkplog.WithLevel(slog.LevelDebug))
	}

	// Create registry client
	clientOpts := &regclient.Options{
		Insecure:      p.pushParams.Insecure,
		TLSSkipVerify: p.pushParams.SkipTLSVerification,
		Logger:        logger,
	}

	if p.pushParams.RegistryAuth != nil {
		clientOpts.Auth = p.pushParams.RegistryAuth
	}

	var client registry.Client
	client = regclient.NewClientWithOptions(p.pushParams.RegistryHost, clientOpts)

	// Scope to the registry path
	if p.pushParams.RegistryPath != "" {
		client = client.WithSegment(p.pushParams.RegistryPath)
	}

	svc := mirror.NewPushService(
		client,
		&mirror.PushServiceOptions{
			BundleDir:  p.pushParams.BundleDir,
			WorkingDir: p.pushParams.WorkingDir,
		},
		logger.Named("push"),
		p.logger.(*log.SLogger),
	)

	err := svc.Push(ctx)
	if err != nil {
		// Handle context cancellation gracefully
		if errors.Is(err, context.Canceled) {
			p.logger.WarnLn("Operation cancelled by user")
			return nil
		}
		return fmt.Errorf("push to registry: %w", err)
	}

	return nil
}

// validateRegistryAccess validates access to the registry
func (p *Pusher) validateRegistryAccess() error {
	p.logger.InfoLn("Validating registry access")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := validateRegistryAccess(ctx, p.pushParams)
	if err != nil && os.Getenv("MIRROR_BYPASS_ACCESS_CHECKS") != "1" {
		return fmt.Errorf("registry credentials validation failure: %w", err)
	}
	return nil
}
