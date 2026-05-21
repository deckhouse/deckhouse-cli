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

package puller

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

// TestPullImages_AllowMissingTagsDoesNotSwallowContextCancellation locks in
// the root-cause fix for the empty 5120-byte module tar bug.
//
// Pre-fix behavior: with AllowMissingTags=true, a context.Canceled returned
// by GetDigest was silently swallowed and the loop continued doing nothing
// useful for every remaining image. The caller then thought the pull
// succeeded, ran the pack phase, and produced empty stub tars for every
// module that had a layout pre-created but no images actually pulled.
//
// Post-fix behavior: context cancellation must propagate immediately so the
// caller knows to stop and never reaches the pack phase with empty state.
func TestPullImages_AllowMissingTagsDoesNotSwallowContextCancellation(t *testing.T) {
	gs := &fakeGetterService{
		getDigest: func(ctx context.Context, _ string) (*v1.Hash, error) {
			return nil, context.Canceled
		},
	}

	cfg := PullConfig{
		Name: "test",
		ImageSet: map[string]*ImageMeta{
			"reg.example/foo:v1": nil,
			"reg.example/bar:v1": nil,
		},
		AllowMissingTags: true, // the trapdoor that used to swallow cancellation
		GetterService:    gs,
	}

	ps := NewPullerService(dkplog.NewLogger(dkplog.WithLevel(slog.LevelError)), log.NewNop())
	err := ps.PullImages(context.Background(), cfg)
	require.ErrorIs(t, err, context.Canceled,
		"context.Canceled from GetDigest must propagate even with AllowMissingTags=true")
}

// TestPullImages_AlreadyCancelledContextBailsBeforeFirstCall ensures that we
// don't spend the user's retry budget on calls that are guaranteed to fail.
func TestPullImages_AlreadyCancelledContextBailsBeforeFirstCall(t *testing.T) {
	var calls atomic.Int32
	gs := &fakeGetterService{
		getDigest: func(ctx context.Context, _ string) (*v1.Hash, error) {
			calls.Add(1)
			return nil, errors.New("must not be called after ctx cancel")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := PullConfig{
		Name:             "test",
		ImageSet:         map[string]*ImageMeta{"reg.example/foo:v1": nil},
		AllowMissingTags: true,
		GetterService:    gs,
	}

	ps := NewPullerService(dkplog.NewLogger(dkplog.WithLevel(slog.LevelError)), log.NewNop())
	err := ps.PullImages(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(0), calls.Load(),
		"GetDigest must not be invoked once the context is already cancelled")
}

// TestPullImages_AllowMissingTagsStillSwallowsRealNotFound covers the
// happy-path use of AllowMissingTags: a genuine "tag not found" must still
// be silently skipped so callers can speculatively probe optional images.
func TestPullImages_AllowMissingTagsStillSwallowsRealNotFound(t *testing.T) {
	gs := &fakeGetterService{
		getDigest: func(ctx context.Context, _ string) (*v1.Hash, error) {
			return nil, errors.New("not found in registry")
		},
	}

	cfg := PullConfig{
		Name:             "test",
		ImageSet:         map[string]*ImageMeta{"reg.example/foo:v1": nil},
		AllowMissingTags: true,
		GetterService:    gs,
	}

	ps := NewPullerService(dkplog.NewLogger(dkplog.WithLevel(slog.LevelError)), log.NewNop())
	err := ps.PullImages(context.Background(), cfg)
	require.NoError(t, err, "AllowMissingTags must continue to swallow regular not-found errors")
}

// fakeGetterService is a tiny pkg.BasicService that lets each test rewire
// only the call it cares about.
type fakeGetterService struct {
	getDigest        func(ctx context.Context, tag string) (*v1.Hash, error)
	getImage         func(ctx context.Context, tag string, opts ...registry.ImageGetOption) (pkg.RegistryImage, error)
	checkImageExists func(ctx context.Context, tag string) error
	listTags         func(ctx context.Context) ([]string, error)
}

func (f *fakeGetterService) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	if f.getDigest != nil {
		return f.getDigest(ctx, tag)
	}
	return nil, errors.New("getDigest not implemented")
}

func (f *fakeGetterService) GetImage(ctx context.Context, tag string, opts ...registry.ImageGetOption) (pkg.RegistryImage, error) {
	if f.getImage != nil {
		return f.getImage(ctx, tag, opts...)
	}
	return nil, errors.New("getImage not implemented")
}

func (f *fakeGetterService) CheckImageExists(ctx context.Context, tag string) error {
	if f.checkImageExists != nil {
		return f.checkImageExists(ctx, tag)
	}
	return errors.New("checkImageExists not implemented")
}

func (f *fakeGetterService) ListTags(ctx context.Context) ([]string, error) {
	if f.listTags != nil {
		return f.listTags(ctx)
	}
	return nil, errors.New("listTags not implemented")
}
