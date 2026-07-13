package render

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	pkgrender "github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/find"
)

var (
	// file filters the output to manifests rendered from the template whose file name equals it.
	file string
	// renderFile, when set, is the path the clean render (no '# Source:' headers) is written to.
	renderFile string
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
  package render --file deployment.yaml --render-file deployment.out.yaml`,
		Args:         cobra.MaximumNArgs(0),
		SilenceUsage: true,
		RunE:         render,
	}

	cmd.Flags().StringVar(&file, "file", "", "Show only manifests rendered from the template with this file name")
	cmd.Flags().StringVar(&renderFile, "render-file", "", "Write the clean render (without '# Source:' headers) to this file path")

	return cmd
}

// render finds the current package, renders its templates, and either prints the
// resulting manifests to stdout or writes a clean render to the --render-file path.
func render(cmd *cobra.Command, _ []string) error {
	path, err := find.PackageDir()
	if err != nil {
		return fmt.Errorf("find package dir: %w", err)
	}

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
