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

package pusher

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// newTestService builds a Service with warning-level loggers suitable for tests.
func newTestService(t *testing.T) *Service {
	t.Helper()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)
	return NewService(logger, userLogger)
}

// =============================================================================
// PackageExists
// =============================================================================

func TestPackageExists_TarFileExists(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "mypackage.tar"))
	require.NoError(t, err)
	f.Close()

	svc := newTestService(t)
	assert.True(t, svc.PackageExists(dir, "mypackage"),
		"PackageExists must return true when <name>.tar is present")
}

func TestPackageExists_ChunkedFileExists(t *testing.T) {
	dir := t.TempDir()
	// chunked layout: first chunk file is <name>.tar.chunk000
	f, err := os.Create(filepath.Join(dir, "mypackage.tar.chunk000"))
	require.NoError(t, err)
	f.Close()

	svc := newTestService(t)
	assert.True(t, svc.PackageExists(dir, "mypackage"),
		"PackageExists must return true when <name>.tar.chunk000 is present")
}

func TestPackageExists_NeitherTarNorChunk(t *testing.T) {
	dir := t.TempDir()

	svc := newTestService(t)
	assert.False(t, svc.PackageExists(dir, "mypackage"),
		"PackageExists must return false when no tar or chunk file exists")
}

func TestPackageExists_UnrelatedFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	// A file with a similar but distinct name must not match.
	f, err := os.Create(filepath.Join(dir, "other.tar"))
	require.NoError(t, err)
	f.Close()

	svc := newTestService(t)
	assert.False(t, svc.PackageExists(dir, "mypackage"),
		"PackageExists must not match unrelated tar files")
}

// =============================================================================
// OpenPackage
// =============================================================================

func TestOpenPackage_TarFileOpened(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "bundle.tar"))
	require.NoError(t, err)
	f.Close()

	svc := newTestService(t)
	rc, err := svc.OpenPackage(dir, "bundle")
	require.NoError(t, err)
	require.NotNil(t, rc)
	rc.Close()
}

func TestOpenPackage_MissingFile_ReturnsError(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.OpenPackage(t.TempDir(), "no-such-package")
	assert.Error(t, err, "OpenPackage must return an error when neither .tar nor chunk files exist")
}

// =============================================================================
// PushLayout
// =============================================================================

// buildAnnotatedLayout creates an OCI layout directory under dir and appends
// an image annotated with io.deckhouse.image.short_tag = tag.
func buildAnnotatedLayout(t *testing.T, dir, tag string) layout.Path {
	t.Helper()

	imgLayout, err := regimage.NewImageLayout(dir)
	require.NoError(t, err, "create OCI layout")

	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.0.0"}`).
		MustBuild()

	lp := imgLayout.Path()
	err = lp.AppendImage(img, layout.WithAnnotations(map[string]string{
		regimage.AnnotationImageShortTag: tag,
	}))
	require.NoError(t, err, "append image to layout")

	return lp
}

// TestPushLayout_PushesImageWithShortTag verifies that PushLayout reads an OCI
// layout, finds the image annotated with io.deckhouse.image.short_tag, and
// pushes it to the destination registry under that tag.
func TestPushLayout_PushesImageWithShortTag(t *testing.T) {
	const pushedTag = "v1.0.0"

	lp := buildAnnotatedLayout(t, t.TempDir(), pushedTag)

	reg := upfake.NewRegistry("push.example.io")
	destClient := pkgclient.Adapt(upfake.NewClient(reg))

	svc := newTestService(t)
	err := svc.PushLayout(context.Background(), lp, destClient)
	require.NoError(t, err)

	// After push the tag must be reachable in the destination client.
	err = destClient.CheckImageExists(context.Background(), pushedTag)
	assert.NoError(t, err,
		"tag %q must exist in the destination registry after PushLayout", pushedTag)
}

// TestPushLayout_SkipsImageWithoutShortTag verifies that PushLayout silently
// skips manifests that lack the io.deckhouse.image.short_tag annotation and
// does not return an error.
func TestPushLayout_SkipsImageWithoutShortTag(t *testing.T) {
	dir := t.TempDir()

	imgLayout, err := regimage.NewImageLayout(dir)
	require.NoError(t, err)

	img := upfake.NewImageBuilder().MustBuild()
	// Append without the short_tag annotation.
	err = imgLayout.Path().AppendImage(img)
	require.NoError(t, err)

	reg := upfake.NewRegistry("push.example.io")
	destClient := pkgclient.Adapt(upfake.NewClient(reg))

	svc := newTestService(t)
	err = svc.PushLayout(context.Background(), imgLayout.Path(), destClient)
	assert.NoError(t, err, "PushLayout must not error when short_tag annotation is absent")
}

// TestPushLayout_EmptyLayout verifies that PushLayout on an empty OCI layout
// completes without error and pushes nothing.
func TestPushLayout_EmptyLayout(t *testing.T) {
	imgLayout, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	reg := upfake.NewRegistry("push.example.io")
	destClient := pkgclient.Adapt(upfake.NewClient(reg))

	svc := newTestService(t)
	err = svc.PushLayout(context.Background(), imgLayout.Path(), destClient)
	assert.NoError(t, err, "PushLayout on an empty layout must not error")
}

// TestPushLayout_MultipleImages verifies that all annotated images in a layout
// are pushed to the destination.
func TestPushLayout_MultipleImages(t *testing.T) {
	dir := t.TempDir()

	imgLayout, err := regimage.NewImageLayout(dir)
	require.NoError(t, err)
	lp := imgLayout.Path()

	tags := []string{"v1.0.0", "v1.1.0", "v1.2.0"}
	for _, tag := range tags {
		img := upfake.NewImageBuilder().
			WithFile("version.json", `{"version":"`+tag+`"}`).
			MustBuild()
		err = lp.AppendImage(img, layout.WithAnnotations(map[string]string{
			regimage.AnnotationImageShortTag: tag,
		}))
		require.NoError(t, err)
	}

	reg := upfake.NewRegistry("push.example.io")
	destClient := pkgclient.Adapt(upfake.NewClient(reg))

	svc := newTestService(t)
	err = svc.PushLayout(context.Background(), lp, destClient)
	require.NoError(t, err)

	for _, tag := range tags {
		err = destClient.CheckImageExists(context.Background(), tag)
		assert.NoErrorf(t, err, "tag %q must exist in destination after PushLayout", tag)
	}
}
