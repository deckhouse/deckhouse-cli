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

package vex

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse/pkg/registry"
)

const (
	hex       = "0b2aec490fe4356e4dba91863456c8f1bfcf92694f2f401be5443fe8fe2b97eb"
	repo      = "registry.example.com:5000/deckhouse/ee"
	digestRef = repo + "@sha256:" + hex
)

func TestLegacyRef(t *testing.T) {
	assert.Equal(t, repo+":sha256-"+hex+".att", LegacyRef(digestRef))
	assert.Equal(t, repo+":v1.2.3.att", LegacyRef(repo+":v1.2.3"))
}

func TestReferrersRef(t *testing.T) {
	ref, ok := ReferrersRef(digestRef)
	require.True(t, ok)
	assert.Equal(t, repo+":sha256-"+hex, ref)

	_, ok = ReferrersRef(repo + ":v1.2.3")
	assert.False(t, ok, "the referrers tag schema is keyed by digest, a tag has no index")
}

func TestIsTag(t *testing.T) {
	cases := map[string]bool{
		"sha256-" + hex + ".att":         true,
		"sha256-" + hex:                  true,
		"v1.2.3.att":                     true,
		hex:                              false, // digest-form image tag
		"v1.2.3":                         false,
		"sha256-" + hex[:63]:             false,
		"sha256-" + strings.ToUpper(hex): false,
		"sha256-" + hex + "0":            false,
	}

	for tag, want := range cases {
		assert.Equalf(t, want, IsTag(tag), "IsTag(%q)", tag)
	}
}

func TestFind(t *testing.T) {
	legacy := Ref{Name: repo + ":sha256-" + hex + ".att"}
	index := Ref{Name: repo + ":sha256-" + hex, Index: true}

	cases := []struct {
		name     string
		imageRef string
		tags     tagSet
		want     []Ref
		asked    []string
	}{
		{
			name:     "both formats",
			imageRef: digestRef,
			tags:     tagSet{"sha256-" + hex + ".att": nil, "sha256-" + hex: nil},
			want:     []Ref{legacy, index},
		},
		{
			name:     "referrers index only",
			imageRef: digestRef,
			tags:     tagSet{"sha256-" + hex: nil},
			want:     []Ref{index},
		},
		{
			name:     "legacy only",
			imageRef: digestRef,
			tags:     tagSet{"sha256-" + hex + ".att": nil},
			want:     []Ref{legacy},
		},
		{
			name:     "none",
			imageRef: digestRef,
			tags:     tagSet{},
			want:     []Ref{},
		},
		{
			name:     "missing repository counts as not found",
			imageRef: digestRef,
			tags:     tagSet{"sha256-" + hex: registry.ErrRepositoryNotFound},
			want:     []Ref{},
		},
		{
			name:     "a tag reference has no referrers index to look up",
			imageRef: repo + ":v1.2.3",
			tags:     tagSet{"v1.2.3.att": nil},
			want:     []Ref{{Name: repo + ":v1.2.3.att"}},
			asked:    []string{"v1.2.3.att"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checker := &recordingChecker{tags: tc.tags}

			got, err := Find(context.Background(), checker, tc.imageRef)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			if tc.asked != nil {
				assert.Equal(t, tc.asked, checker.asked)
			}
		})
	}
}

func TestFind_FailsOnRegistryError(t *testing.T) {
	denied := fmt.Errorf("%w: 403", registry.ErrAccessDenied)
	checker := &recordingChecker{tags: tagSet{"sha256-" + hex: denied}}

	_, err := Find(context.Background(), checker, digestRef)
	require.Error(t, err)
	assert.ErrorIs(t, err, registry.ErrAccessDenied)
	assert.ErrorContains(t, err, repo+":sha256-"+hex)
}

// tagSet maps the tags a repository has to the error checking them returns;
// any other tag is not found.
type tagSet map[string]error

type recordingChecker struct {
	tags  tagSet
	asked []string
}

func (c *recordingChecker) CheckImageExists(_ context.Context, tag string) error {
	c.asked = append(c.asked, tag)

	if err, ok := c.tags[tag]; ok {
		return err
	}

	return fmt.Errorf("%w: %s", registry.ErrImageNotFound, tag)
}
