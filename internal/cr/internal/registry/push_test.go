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

package registry

import (
	"context"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// Push must reject nil inputs explicitly instead of letting them reach
// remote.Write / t.Digest() inside go-containerregistry, which would
// panic on a nil receiver.
func TestPush_RejectsNilObject(t *testing.T) {
	opts := New()
	cases := []struct {
		name string
		fn   func() error
	}{
		{
			name: "literal nil",
			fn: func() error {
				_, err := Push(context.Background(), "example.com/repo:v1", nil, opts)
				return err
			},
		},
		{
			name: "uninitialized v1.Image",
			fn: func() error {
				var nilImage v1.Image
				_, err := Push(context.Background(), "example.com/repo:v1", nilImage, opts)
				return err
			},
		},
		{
			name: "uninitialized v1.ImageIndex",
			fn: func() error {
				var nilIndex v1.ImageIndex
				_, err := Push(context.Background(), "example.com/repo:v1", nilIndex, opts)
				return err
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.fn()
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "object is nil") {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
