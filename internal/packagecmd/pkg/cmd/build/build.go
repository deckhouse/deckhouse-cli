package build

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/builder"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/logs"
)

var (
	// repository stores the target registry repository for build output.
	repository string
	// repositoryUser stores the registry username used for authentication.
	repositoryUser string
	// repositoryToken stores the registry token used for authentication.
	repositoryToken string
	// packageVersion stores the semantic package version to build and publish.
	packageVersion string
	// force controls whether an existing version can be overwritten.
	force bool
	// debug enables verbose build output and keeps rendered Werf templates.
	debug bool
	// sign controls whether the built package is signed.
	sign bool
	// signCert stores a signing certificate path or base64-encoded certificate.
	signCert string
	// signKey stores a signing key path, base64-encoded key, or vault URL.
	signKey string
)

// NewCmdBuild creates a command that builds package images with Werf, pushes
// them to a container registry, and optionally signs the published package.
func NewCmdBuild() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build --version <version> [--repository <repository>] [--force]",
		Short: "Build and push a package to a container registry",
		Long: `Build a package using Werf and push it to a container registry.

This command:
  • Validates the package structure (package.yaml)
  • Builds container images using Werf in a Docker container
  • Pushes the built images to the specified registry
  • Tags images with semantic version

Environment Variables:
  PACKAGE_BUILD_REPOSITORY          Registry URL for build command
  PACKAGE_BUILD_REPOSITORY_USER     Registry username for authentication
  PACKAGE_BUILD_REPOSITORY_TOKEN    Registry token for authentication
`,
		Example: `
  # Build with explicit registry
  package build -r ghcr.io/org/packages/my-pkg --version=v1.0.0

  # Build using environment variables
  export PACKAGE_BUILD_REPOSITORY=ghcr.io/org/packages
  package build --version=v1.0.0

  # Build with authentication
  package build -r registry.io/packages/app -u myuser -t mytoken --version=v1.0.0

  # Force overwrite an existing version
  package build --version=v1.0.0 --force

  # Build with debug mode (keeps rendered werf templates)
  package build --version=v1.0.0 --debug`,
		Args:         cobra.ExactArgs(0),
		SilenceUsage: true,
		RunE:         build,
	}

	cmd.Flags().StringVarP(&repository, "repo", "r", "", "Repository (env: PACKAGE_BUILD_REPOSITORY)")
	cmd.Flags().StringVarP(&repositoryUser, "user", "u", "", "Registry user (env: PACKAGE_BUILD_REPOSITORY_USER)")
	cmd.Flags().StringVarP(&repositoryToken, "token", "t", "", "Registry token (env: PACKAGE_BUILD_REPOSITORY_TOKEN)")
	cmd.Flags().StringVarP(&packageVersion, "version", "v", "", "Package version")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "force update version in registry")
	cmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")

	cmd.Flags().StringVar(&signCert, "sign-cert", "", "sign certificate path or base64 string (env: PACKAGE_BUILD_SIGN_CERT)")
	cmd.Flags().StringVar(&signKey, "sign-key", "", "sign key path or base64 string or vault url (env: PACKAGE_BUILD_SIGN_KEY)")
	cmd.Flags().BoolVar(&sign, "sign", false, "sign package with certificate and key. --sign-cert and --sign-key are required")

	if err := cmd.MarkFlagRequired("version"); err != nil {
		panic(err)
	}

	return cmd
}

// build runs the package build workflow using command flags and environment defaults.
func build(cmd *cobra.Command, _ []string) error {
	logger := logs.New(true)

	// Use environment variables as defaults if flags are not set
	if repository == "" {
		repository = os.Getenv("PACKAGE_BUILD_REPOSITORY")
	}

	if repositoryUser == "" {
		repositoryUser = os.Getenv("PACKAGE_BUILD_REPOSITORY_USER")
	}

	if repositoryToken == "" {
		repositoryToken = os.Getenv("PACKAGE_BUILD_REPOSITORY_TOKEN")
	}

	if signCert == "" {
		signCert = os.Getenv("PACKAGE_BUILD_SIGN_CERT")
	}

	if signKey == "" {
		signKey = os.Getenv("PACKAGE_BUILD_SIGN_KEY")
	}

	if sign {
		if signCert == "" || signKey == "" {
			return fmt.Errorf("--sign-cert and --sign-key are required with --sign")
		}
	}

	if len(packageVersion) == 0 {
		return fmt.Errorf("version is required")
	}

	opts := builder.Options{
		Force: force,
		Debug: debug,
		Credentials: builder.Credentials{
			Repository: repository,
			Username:   repositoryUser,
			Token:      repositoryToken,
		},
		Sign: builder.SignOptions{
			Enabled: sign,
			Cert:    signCert,
			Key:     signKey,
		},
	}

	ctx := cmd.Context()
	if err := builder.Build(ctx, packageVersion, opts, logger); err != nil {
		return fmt.Errorf("build package: %w", err)
	}

	return nil
}
