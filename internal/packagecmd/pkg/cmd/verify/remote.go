package verify

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify"
)

var (
	// remoteVersion is the published version to verify. Empty verifies the latest one.
	remoteVersion string
	// remoteRelease additionally verifies the release image published for the version.
	remoteRelease bool
)

// NewCmdVerifyRemote creates a command that verifies a published package by its
// registry path and name, reading the images from the registry instead of the
// working directory.
func NewCmdVerifyRemote() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remote <registry-path> <package>",
		Short: "Verify a published package from a registry",
		Long: `Verify a published package.

The arguments are the registry path the package is published under and the package
name. Build publishes two images per version, both tagged with that version:

  bundle   <registry-path>/<package>:<version>
           checked against the 'remote.bundle' linter settings
  release  <registry-path>/<package>/version:<version>
           checked against the 'remote.release' linter settings

Without --version the latest published version is verified, chosen by comparing the
repository tags as semver. The bundle image carries the package itself and is always
verified; add --release to verify the release image alongside it, in which case both
are verified concurrently and each must exist.

Lint settings are read from the working directory, not from the image: published
images do not carry a .pkglint.yaml.`,
		Example: `
  # Verify the latest published version
  package verify remote registry.io/packages app

  # Verify a specific version
  package verify remote registry.io/packages app --version 1.0.0

  # Verify the bundle and release images together
  package verify remote registry.io/packages app --version 1.0.0 --release

  # Verify a published package with an explicit lint config
  package verify remote registry.io/packages app --lint-config ./configs/pkglint.yaml`,
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			registryPath, packageName := args[0], args[1]

			return verify.Verify(cmd.Context(), options(verify.RemoteTarget{
				Repository: registryPath + "/" + packageName,
				Version:    remoteVersion,
				Release:    remoteRelease,
			}))
		},
	}

	cmd.Flags().StringVar(&remoteVersion, "version", "", "Version to verify (default: the latest published version)")
	cmd.Flags().BoolVar(&remoteRelease, "release", false, "Also verify the release image published for the version")

	return cmd
}
