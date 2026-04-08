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

package stub_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	dkpclient "github.com/deckhouse/deckhouse/pkg/registry/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/stub"
)

// ----- helpers -----

func newFilledRegistry(host string) *stub.Registry {
	reg := stub.NewRegistry(host)
	reg.MustAddImage("deckhouse/ee", "v1.65.0",
		stub.NewImageBuilder().
			WithFile("version.json", `{"version":"v1.65.0"}`).
			WithLabel("org.opencontainers.image.version", "v1.65.0").
			MustBuild(),
	)
	reg.MustAddImage("deckhouse/ee", "v1.64.0",
		stub.NewImageBuilder().
			WithFile("version.json", `{"version":"v1.64.0"}`).
			MustBuild(),
	)
	reg.MustAddImage("deckhouse/ee/release-channel", "stable",
		stub.NewImageBuilder().
			WithFile("version.json", `{"version":"v1.64.0"}`).
			MustBuild(),
	)
	return reg
}

// ----- WithSegment / GetRegistry -----

// GetRegistry returns the HOST portion of the current path (not host+repo).
// This matches the upstream contract where GetRegistry returns the registry host.

func TestClient_WithSegment_ChainedPaths(t *testing.T) {
	reg := stub.NewRegistry("gcr.io")
	client := stub.NewClient(reg)

	// WithSegment appends to the path; GetRegistry returns only the host part.
	scoped := client.WithSegment("org").WithSegment("repo")
	assert.Equal(t, "gcr.io", scoped.GetRegistry())
}

func TestClient_WithSegment_MultiSegments(t *testing.T) {
	reg := stub.NewRegistry("gcr.io")
	client := stub.NewClient(reg)

	scoped := client.WithSegment("org", "repo", "sub")
	assert.Equal(t, "gcr.io", scoped.GetRegistry())
}

func TestClient_GetRegistry_DefaultHost(t *testing.T) {
	reg := stub.NewRegistry("reg.example.com")
	client := stub.NewClient(reg)

	assert.Equal(t, "reg.example.com", client.GetRegistry())
}

func TestClient_WithSegment_ScopeListTags(t *testing.T) {
	reg := stub.NewRegistry("gcr.io")
	img := stub.NewImageBuilder().WithFile("f.txt", "x").MustBuild()
	reg.MustAddImage("org/repo", "v1", img)
	client := stub.NewClient(reg)

	scoped := client.WithSegment("org").WithSegment("repo")
	tags, err := scoped.ListTags(context.Background())
	require.NoError(t, err)
	assert.Contains(t, tags, "v1")
}

// ----- GetDigest -----

func TestClient_GetDigest_ExistingTag(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	hash, err := client.GetDigest(context.Background(), "v1.65.0")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(hash.String(), "sha256:"))
}

func TestClient_GetDigest_MissingTag(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	_, err := client.GetDigest(context.Background(), "does-not-exist")
	require.Error(t, err)
	assert.True(t, errors.Is(err, dkpclient.ErrImageNotFound))
}

// ----- GetManifest -----

func TestClient_GetManifest_ExistingTag(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	manifest, err := client.GetManifest(context.Background(), "v1.65.0")
	require.NoError(t, err)
	require.NotNil(t, manifest)
}

func TestClient_GetManifest_MissingTag(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	_, err := client.GetManifest(context.Background(), "missing")
	require.Error(t, err)
}

// ----- GetImageConfig -----

func TestClient_GetImageConfig_ExistingTag(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	cfg, err := client.GetImageConfig(context.Background(), "v1.65.0")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "v1.65.0", cfg.Config.Labels["org.opencontainers.image.version"])
}

// ----- CheckImageExists -----

func TestClient_CheckImageExists_Present(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	err := client.CheckImageExists(context.Background(), "v1.65.0")
	assert.NoError(t, err)
}

func TestClient_CheckImageExists_Absent(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	err := client.CheckImageExists(context.Background(), "v2.0.0")
	require.Error(t, err)
	assert.True(t, errors.Is(err, dkpclient.ErrImageNotFound))
}

// ----- GetImage -----

func TestClient_GetImage_ByTag(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	img, err := client.GetImage(context.Background(), "v1.65.0")
	require.NoError(t, err)
	require.NotNil(t, img)
}

func TestClient_GetImage_ByDigest(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	// Retrieve digest first.
	hash, err := client.GetDigest(context.Background(), "v1.65.0")
	require.NoError(t, err)

	// Look up by digest reference (@sha256:...).
	img, err := client.GetImage(context.Background(), "@"+hash.String())
	require.NoError(t, err)
	require.NotNil(t, img)
}

func TestClient_GetImage_MissingTag(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	_, err := client.GetImage(context.Background(), "missing-tag")
	require.Error(t, err)
	assert.True(t, errors.Is(err, dkpclient.ErrImageNotFound))
}

// ----- ListTags -----

func TestClient_ListTags(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	tags, err := client.ListTags(context.Background())
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"v1.65.0", "v1.64.0"}, tags)
}

func TestClient_ListTags_EmptyRepo(t *testing.T) {
	reg := stub.NewRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("no-such-repo")

	tags, err := client.ListTags(context.Background())
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// ----- ListRepositories -----

func TestClient_ListRepositories_AllUnderHost(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg)

	repos, err := client.ListRepositories(context.Background())
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"deckhouse/ee",
		"deckhouse/ee/release-channel",
	}, repos)
}

func TestClient_ListRepositories_Scoped(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse")

	repos, err := client.ListRepositories(context.Background())
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"deckhouse/ee",
		"deckhouse/ee/release-channel",
	}, repos)
}

// ----- DeleteTag -----

func TestClient_DeleteTag_Existing(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	require.NoError(t, client.DeleteTag(context.Background(), "v1.65.0"))

	err := client.CheckImageExists(context.Background(), "v1.65.0")
	require.Error(t, err)
}

func TestClient_DeleteTag_Missing(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	err := client.DeleteTag(context.Background(), "does-not-exist")
	require.Error(t, err)
	assert.True(t, errors.Is(err, dkpclient.ErrImageNotFound))
}

// ----- TagImage -----

func TestClient_TagImage(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	require.NoError(t, client.TagImage(context.Background(), "v1.65.0", "stable"))

	// "stable" should now resolve to the same digest as "v1.65.0".
	origDigest, err := client.GetDigest(context.Background(), "v1.65.0")
	require.NoError(t, err)
	newDigest, err := client.GetDigest(context.Background(), "stable")
	require.NoError(t, err)

	assert.Equal(t, origDigest.String(), newDigest.String())
}

func TestClient_TagImage_SourceMissing(t *testing.T) {
	reg := newFilledRegistry("gcr.io")
	client := stub.NewClient(reg).WithSegment("deckhouse", "ee")

	err := client.TagImage(context.Background(), "no-such-tag", "dest")
	require.Error(t, err)
}

// ----- PushImage -----

func TestClient_PushImage_NewTag(t *testing.T) {
	reg := stub.NewRegistry("push.io")
	client := stub.NewClient(reg).WithSegment("org", "app")

	img := stub.NewImageBuilder().WithFile("app.txt", "app v1").MustBuild()

	require.NoError(t, client.PushImage(context.Background(), "v2", img))

	tags, err := client.ListTags(context.Background())
	require.NoError(t, err)
	assert.Contains(t, tags, "v2")
}

func TestClient_PushImage_AutoCreatesRegistry(t *testing.T) {
	// PushImage should auto-create a new registry entry if the host is unknown.
	reg := stub.NewRegistry("known.io")
	client := stub.NewClient(reg)

	img := stub.NewImageBuilder().MustBuild()

	scopedToUnknown := client.WithSegment("unknown.io", "repo")
	require.NoError(t, scopedToUnknown.PushImage(context.Background(), "v1", img))
}

// ----- Cross-registry routing -----

func TestClient_CrossRegistryRouting(t *testing.T) {
	regSrc := stub.NewRegistry("src.io")
	regDst := stub.NewRegistry("dst.io")

	imgSrc := stub.NewImageBuilder().WithFile("src.txt", "source").MustBuild()
	imgDst := stub.NewImageBuilder().WithFile("dst.txt", "dest").MustBuild()

	regSrc.MustAddImage("lib", "v1", imgSrc)
	regDst.MustAddImage("lib", "v1", imgDst)

	clientSrc := stub.NewClient(regSrc)
	clientDst := stub.NewClient(regDst)

	// The default registry path is reported correctly for each client.
	assert.Equal(t, "src.io", clientSrc.GetRegistry())
	assert.Equal(t, "dst.io", clientDst.GetRegistry())

	tagsFromSrc, err := clientSrc.WithSegment("lib").ListTags(context.Background())
	require.NoError(t, err)
	assert.Contains(t, tagsFromSrc, "v1")

	tagsFromDst, err := clientDst.WithSegment("lib").ListTags(context.Background())
	require.NoError(t, err)
	assert.Contains(t, tagsFromDst, "v1")
}
