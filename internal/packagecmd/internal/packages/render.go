package packages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/values"
)

const (
	// imagesDirName is the package subdirectory holding one directory per image.
	imagesDirName = "images"

	// dummyDigest is a placeholder OCI digest used until real digests are wired in.
	dummyDigest = "sha256:000000000000000"
)

// Remote describes the OCI registry a package is published to. It is
// embedded into the runtime context surfaced to templates so manifests
// can reference the registry and its credentials.
type Remote struct {
	Repository   string `json:"repository" yaml:"repository"`
	DockerConfig string `json:"dockercfg" yaml:"dockercfg"`
}

// Render loads the package at path and renders it to a slice of resource
// objects: it reads the values schemas, scans images/ for per-image
// digests, builds the runtime context, and invokes the Helm renderer.
func Render(ctx context.Context, def Definition, path string) ([]render.Object, error) {
	valuesStore, err := values.LoadStorage(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load values: %w", err)
	}

	digests, err := loadDigests(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load digests: %w", err)
	}

	repo := Remote{
		Repository:   "registry.io",
		DockerConfig: "somedockercfg",
	}

	valuesPath, err := createTmpValuesFile("default.test", valuesStore.GetValues())
	if err != nil {
		return nil, fmt.Errorf("create temp values file: %w", err)
	}
	defer os.Remove(valuesPath)

	marshalled, _ := json.Marshal(buildRuntimeValues(def, repo, digests, valuesStore.GetSettings()))

	opts := render.Options{
		Path:                path,
		ValuesPaths:         []string{valuesPath},
		RootValues:          fmt.Sprintf("Application=%s", marshalled),
		ExtraCapabilitities: []string{"autoscaling.k8s.io/v1/VerticalPodAutoscaler"},
	}

	result, err := render.Render(ctx, "test", "default.test", opts)
	if err != nil {
		return nil, fmt.Errorf("render helm chart: %w", err)
	}

	return result, nil
}

// loadDigests returns one entry per immediate subdirectory of <path>/images,
// keyed by directory name. A missing images/ directory yields an empty map.
func loadDigests(path string) (map[string]string, error) {
	digests := make(map[string]string)

	entries, err := os.ReadDir(filepath.Join(path, imagesDirName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return digests, nil
		}

		return nil, fmt.Errorf("read images dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			digests[entry.Name()] = dummyDigest
		}
	}

	return digests, nil
}

// buildRuntimeValues returns the runtime context surfaced to templates:
// Instance (name/namespace), Package (image references plus metadata),
// and Settings (user-supplied values).
func buildRuntimeValues(def Definition, repo Remote, digests map[string]string, settings map[string]any) any {
	images := make(map[string]string, len(digests))
	for name, digest := range digests {
		images[name] = fmt.Sprintf("%s/%s@%s", repo.Repository, def.Name, digest)
	}

	return struct {
		Instance map[string]any
		Package  map[string]any
		Settings map[string]any
	}{
		Instance: map[string]any{
			"Name":      "test",
			"Namespace": "default",
		},
		Package: map[string]any{
			"Name":     def.Name,
			"Images":   images,
			"Registry": repo,
			"Version":  "dev",
		},
		Settings: settings,
	}
}

// createTmpValuesFile writes values to a uniquely-named YAML file in the
// system temp dir. Caller must os.Remove the returned path.
func createTmpValuesFile(name string, values map[string]any) (string, error) {
	marshalled, err := yaml.Marshal(values)
	if err != nil {
		return "", err
	}

	tmpName := fmt.Sprintf("%s.package-values.yaml-%s", name, uuid.New().String())
	path := filepath.Join(os.TempDir(), tmpName)

	if err := os.WriteFile(path, marshalled, 0600); err != nil {
		return "", fmt.Errorf("failed to dump values: %w", err)
	}

	return path, nil
}
