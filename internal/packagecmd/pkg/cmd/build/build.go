package build

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/builder"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/logs"
)

var (
	// repository stores the target registry repository for build output.
	repository string
	// repositoryUser stores the registry username used for authentication.
	repositoryUser string
	// repositoryToken stores the registry token used for authentication.
	repositoryToken string
	// finalRepository stores the final registry repository for the published artifact.
	finalRepository string
	// finalRepositoryUser stores the final registry username used for authentication.
	finalRepositoryUser string
	// finalRepositoryToken stores the final registry token used for authentication.
	finalRepositoryToken string
	// packageVersion stores the semantic package version to build and publish.
	packageVersion string
	// force controls whether an existing version can be overwritten.
	force bool
	// debug enables verbose build output and keeps rendered Werf templates.
	debug bool
	// insecure allows plain HTTP registries and skips TLS certificate verification.
	insecure bool
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
  PACKAGE_BUILD_REPOSITORY                Registry URL for build command
  PACKAGE_BUILD_REPOSITORY_USER           Registry username for authentication
  PACKAGE_BUILD_REPOSITORY_TOKEN          Registry token for authentication
  PACKAGE_BUILD_FINAL_REPOSITORY          Final registry URL for the published artifact
  PACKAGE_BUILD_FINAL_REPOSITORY_USER     Final registry username for authentication
  PACKAGE_BUILD_FINAL_REPOSITORY_TOKEN    Final registry token for authentication
  PACKAGE_BUILD_INSECURE                  Allow plain HTTP and skip TLS verification
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
  package build --version=v1.0.0 --debug

  # Build against a registry without valid TLS
  package build -r 10.0.0.5:5000/packages --version=v1.0.0 --insecure`,
		Args:         cobra.ExactArgs(0),
		SilenceUsage: true,
		RunE:         build,
	}

	cmd.Flags().StringVarP(&repository, "repo", "r", "", "Repository (env: PACKAGE_BUILD_REPOSITORY)")
	cmd.Flags().StringVarP(&repositoryUser, "user", "u", "", "Registry user (env: PACKAGE_BUILD_REPOSITORY_USER)")
	cmd.Flags().StringVarP(&repositoryToken, "token", "t", "", "Registry token (env: PACKAGE_BUILD_REPOSITORY_TOKEN)")
	cmd.Flags().StringVar(&finalRepository, "final-repo", "", "Final repository (env: PACKAGE_BUILD_FINAL_REPOSITORY)")
	cmd.Flags().StringVar(&finalRepositoryUser, "final-user", "", "Final registry user (env: PACKAGE_BUILD_FINAL_REPOSITORY_USER)")
	cmd.Flags().StringVar(&finalRepositoryToken, "final-token", "", "Final registry token (env: PACKAGE_BUILD_FINAL_REPOSITORY_TOKEN)")
	cmd.Flags().StringVarP(&packageVersion, "version", "v", "", "Package version")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "force update version in registry")
	cmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")
	cmd.Flags().BoolVar(&insecure, "insecure", false, "Allow plain HTTP and skip TLS verification for every registry used by the build, including the final repository and base-image pulls (env: PACKAGE_BUILD_INSECURE)")

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

	if finalRepository == "" {
		finalRepository = os.Getenv("PACKAGE_BUILD_FINAL_REPOSITORY")
	}

	if finalRepositoryUser == "" {
		finalRepositoryUser = os.Getenv("PACKAGE_BUILD_FINAL_REPOSITORY_USER")
	}

	if finalRepositoryToken == "" {
		finalRepositoryToken = os.Getenv("PACKAGE_BUILD_FINAL_REPOSITORY_TOKEN")
	}

	if signCert == "" {
		signCert = os.Getenv("PACKAGE_BUILD_SIGN_CERT")
	}

	if signKey == "" {
		signKey = os.Getenv("PACKAGE_BUILD_SIGN_KEY")
	}

	// The env var is a fallback for an unset flag only: an explicit
	// --insecure=false must win over PACKAGE_BUILD_INSECURE=true.
	if !cmd.Flags().Changed("insecure") {
		insecure, _ = strconv.ParseBool(os.Getenv("PACKAGE_BUILD_INSECURE"))
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
		Force:    force,
		Debug:    debug,
		Insecure: insecure,
		RepositoryCredentials: builder.Credentials{
			Repository: repository,
			Username:   repositoryUser,
			Token:      repositoryToken,
		},
		FinalRepositoryCredentials: builder.Credentials{
			Repository: finalRepository,
			Username:   finalRepositoryUser,
			Token:      finalRepositoryToken,
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
