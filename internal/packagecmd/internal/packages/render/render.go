package render

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/werf/nelm/pkg/action"
	"github.com/werf/nelm/pkg/common"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Options contains options for installing a Helm chart
type Options struct {
	Path        string   // Path to the chart directory
	ValuesPaths []string // Paths to values files
	RootValues  string   // Values in JSON format

	ExtraCapabilitities []string // Extra capabilities
}

type Object struct {
	FilePath string
	*unstructured.Unstructured
}

// ObjectID returns a stable identifier string for diagnostics in the form
// Kind/Namespace/Name (Kind/Name when cluster-scoped).
func (o Object) ObjectID() string {
	ns := o.GetNamespace()
	if ns == "" {
		return fmt.Sprintf("%s/%s", o.GetKind(), o.GetName())
	}

	return fmt.Sprintf("%s/%s/%s", o.GetKind(), ns, o.GetName())
}

// Render renders a nelm chart to YAML manifests without installing it
// Returns the rendered manifests as a YAML string
func Render(ctx context.Context, namespace, releaseName string, opts Options) ([]Object, error) {
	if !isHelmChart(opts.Path) {
		return nil, nil
	}

	var valuesSet []string
	if len(opts.RootValues) > 0 {
		valuesSet = append(valuesSet, opts.RootValues)
	}

	res, err := action.ChartRender(ctx, action.ChartRenderOptions{
		ValuesOptions: common.ValuesOptions{
			ValuesFiles: opts.ValuesPaths,
			RootSetJSON: valuesSet,
		},
		OutputFilePath:         "/dev/null", // No output file, we return the manifest as a string
		Chart:                  opts.Path,
		DefaultChartName:       releaseName,
		DefaultChartVersion:    "0.2.0",
		DefaultChartAPIVersion: "v2",
		ReleaseName:            releaseName,
		ReleaseNamespace:       namespace,
		ExtraAPIVersions:       opts.ExtraCapabilitities,
	})
	if err != nil {
		return nil, fmt.Errorf("render nelm chart '%s': %w", opts.Path, err)
	}

	result := make([]Object, 0, len(res.Resources))

	for _, resource := range res.Resources {
		_, path, _ := strings.Cut(resource.FilePath, "/")
		result = append(result, Object{
			FilePath:     path,
			Unstructured: resource.Unstruct,
		})
	}

	return result, nil
}

// isHelmChart checks if a directory contains a valid Helm chart.
func isHelmChart(path string) bool {
	if _, err := os.Stat(filepath.Join(path, "templates")); err == nil {
		return true
	}

	return false
}
