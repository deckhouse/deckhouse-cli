package render

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	pkgrender "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/find"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/imagefs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/tools/registry"
)

var (
	// file filters the output to manifests rendered from the template whose file name equals it.
	file string
	// renderFile, when set, is the path the clean render (no '# Source:' headers) is written to.
	renderFile string

	// remote is an optional published bundle image, <registry-path>/<package>:<version>,
	// rendered instead of the local package directory.
	remote string
	// remoteUser is the registry username used to pull the remote image.
	remoteUser string
	// remotePassword is the registry password or token used to pull the remote image.
	remotePassword string
)

const (
	// envRemoteUser is the environment fallback for --remote-user.
	envRemoteUser = "PACKAGE_REMOTE_USER"
	// envRemotePassword is the environment fallback for --remote-password.
	envRemotePassword = "PACKAGE_REMOTE_PASSWORD"
)

// NewCmdRender creates a command that renders a package's templates to
// Kubernetes manifests and prints them to stdout using stubbed runtime values.
func NewCmdRender() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "render",
		Short: "Render package templates to Kubernetes manifests",
		Long: `Render a package's templates to Kubernetes manifests using stubbed runtime values.

Use 'package render' in a package directory to preview the manifests that would be
installed in a cluster. Image digests, registry and credentials are stubbed, so the
output is a smoke-check preview, not a faithful representation of a real cluster.

Use 'package render --remote <registry-path>/<package>:<version>' to render a
published bundle image instead of the current directory. The image must exist and
must carry an explicit tag or digest. Only the bundle image is renderable: the
release image ships without a templates directory.

Registry credentials default to the ambient Docker keychain. Pass --remote-user and
--remote-password to authenticate explicitly, or set them in the environment:

  PACKAGE_REMOTE_USER      fallback for --remote-user
  PACKAGE_REMOTE_PASSWORD  fallback for --remote-password

Use --file to print only the manifests rendered from the template with that file
name, and --render-file to write a clean render (without '# Source:' headers) to a file.`,
		Example: `
  # Render the current package
  package render

  # Render and dry-run apply with kubectl
  package render | kubectl apply --dry-run=client -f -

  # Print only the manifests from one template
  package render --file deployment.yaml

  # Write a clean render (no '# Source:' headers) to a file
  package render --render-file rendered.yaml

  # Write a clean render of a single template to a file
  package render --file deployment.yaml --render-file deployment.out.yaml

  # Render a published bundle image
  package render --remote registry.io/packages/app:1.0.0

  # Render one template of a published bundle image
  package render --remote registry.io/packages/app:1.0.0 --file deployment.yaml

  # Render a published bundle image with explicit registry credentials
  package render --remote registry.io/packages/app:1.0.0 --remote-user robot --remote-password s3cret`,
		Args:         cobra.MaximumNArgs(0),
		SilenceUsage: true,
		RunE:         render,
	}

	cmd.Flags().StringVar(&file, "file", "", "Show only manifests rendered from the template with this file name")
	cmd.Flags().StringVar(&renderFile, "render-file", "", "Write the clean render (without '# Source:' headers) to this file path")
	cmd.Flags().StringVarP(&remote, "remote", "r", "",
		"Render a published bundle image by its reference, <registry-path>/<package>:<version>")
	cmd.Flags().StringVar(&remoteUser, "remote-user", "",
		"Registry user for --remote (env: "+envRemoteUser+")")
	cmd.Flags().StringVar(&remotePassword, "remote-password", "",
		"Registry password or token for --remote (env: "+envRemotePassword+")")

	return cmd
}

// render resolves the package source, renders its templates, and either prints the
// resulting manifests to stdout or writes a clean render to the --render-file path.
func render(cmd *cobra.Command, _ []string) error {
	path, cleanup, err := resolvePath(cmd.Context())
	if err != nil {
		return err
	}
	defer cleanup()

	def, err := packages.LoadDefinitionByDir(path)
	if err != nil {
		return fmt.Errorf("load definition: %w", err)
	}

	objects, err := packages.Render(cmd.Context(), def, path)
	if err != nil {
		return fmt.Errorf("render templates: %w", err)
	}

	if file != "" {
		objects = filterByFile(objects, file)
		if len(objects) == 0 {
			return fmt.Errorf("no rendered template matches %q", file)
		}
	}

	if renderFile != "" {
		return writeRenderFile(renderFile, objects)
	}

	return writeObjects(os.Stdout, objects, true)
}

// resolvePath returns the package directory to render and a cleanup function to call
// when rendering is done. Without --remote the directory is discovered from the current
// working directory and cleanup is a no-op; with --remote the published bundle image is
// extracted into a temp directory that cleanup removes.
func resolvePath(ctx context.Context) (string, func(), error) {
	if remote == "" {
		if remoteUser != "" || remotePassword != "" {
			return "", nil, errors.New("--remote-user and --remote-password require --remote")
		}

		path, err := find.PackageDir()
		if err != nil {
			return "", nil, fmt.Errorf("find package dir: %w", err)
		}

		return path, func() {}, nil
	}

	if !hasTag(remote) {
		return "", nil, errors.New("remote reference must carry a tag, <registry-path>/<package>:<version>")
	}

	auth := registry.WithBasicAuth(credentials())

	// Checked before pulling so a typo fails fast instead of after a needless download.
	if err := registry.Exists(ctx, remote, auth); err != nil {
		return "", nil, fmt.Errorf("bundle image: %w", err)
	}

	path, err := imagefs.ExtractToTemp(ctx, remote, auth)
	if err != nil {
		return "", nil, fmt.Errorf("extract bundle image: %w", err)
	}

	return path, func() {
		if err := os.RemoveAll(path); err != nil {
			fmt.Fprintf(os.Stderr, "WARN remove bundle temp directory: %s\n", err)
		}
	}, nil
}

// credentials returns the registry user and password, falling back to the environment
// when the flags are unset. Empty values mean the ambient Docker keychain is used.
func credentials() (string, string) {
	user := remoteUser
	if user == "" {
		user = os.Getenv(envRemoteUser)
	}

	password := remotePassword
	if password == "" {
		password = os.Getenv(envRemotePassword)
	}

	return user, password
}

// hasTag reports whether ref carries an explicit tag or digest. Only the last path
// element is inspected, since a registry host may itself contain a port.
func hasTag(ref string) bool {
	last := ref
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		last = ref[i+1:]
	}

	return strings.ContainsAny(last, ":@")
}

// filterByFile returns the objects rendered from a template whose file name equals name.
func filterByFile(objects []pkgrender.Object, name string) []pkgrender.Object {
	matched := make([]pkgrender.Object, 0, len(objects))
	for _, obj := range objects {
		if filepath.Base(obj.FilePath) == name {
			matched = append(matched, obj)
		}
	}

	return matched
}

// writeRenderFile writes a clean render (no '# Source:' headers) of objects to path.
func writeRenderFile(path string, objects []pkgrender.Object) error {
	var buf bytes.Buffer
	if err := writeObjects(&buf, objects, false); err != nil {
		return err
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write render file: %w", err)
	}

	return nil
}

// writeObjects writes each object as a '---'-separated YAML document. When
// withSource is true, each document is prefixed with a '# Source: <template>'
// comment naming the template it was rendered from.
func writeObjects(w io.Writer, objects []pkgrender.Object, withSource bool) error {
	for _, obj := range objects {
		manifest, err := yaml.Marshal(obj.Object)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", obj.ObjectID(), err)
		}

		if withSource {
			_, err = fmt.Fprintf(w, "---\n# Source: %s\n%s", obj.FilePath, manifest)
		} else {
			_, err = fmt.Fprintf(w, "---\n%s", manifest)
		}

		if err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}

	return nil
}
