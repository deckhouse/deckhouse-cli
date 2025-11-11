/*
Copyright 2025 Flant JSC

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
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/deckhouse/deckhouse-cli/pkg"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// ImagePutter is a function type for putting images to the registry
type ImagePutter func(ctx context.Context, tag string, img v1.Image, opts ...pkg.ImagePutOption) error

// PushConfig encapsulates the configuration for pushing images
type PushConfig struct {
	Name          string
	ImageSet      map[string]struct{}
	Layout        *regimage.ImageLayout
	PutterService pkg.RegistryClient
}

// SplitImageRefByRepoAndTag splits an image reference into repository and tag parts
func SplitImageRefByRepoAndTag(imageReferenceString string) (string, string) {
	splitIndex := strings.LastIndex(imageReferenceString, ":")
	repo := imageReferenceString[:splitIndex]
	tag := imageReferenceString[splitIndex+1:]

	if strings.HasSuffix(repo, "@sha256") {
		repo = strings.TrimSuffix(repo, "@sha256")
		tag = "@sha256:" + tag
	}

	return repo, tag
}
