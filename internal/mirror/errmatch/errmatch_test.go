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

package errmatch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/stretchr/testify/assert"
)

func TestIsImageNotFound_TypedError(t *testing.T) {
	err := &transport.Error{
		StatusCode: 404,
		Errors:     []transport.Diagnostic{{Code: transport.ManifestUnknownErrorCode, Message: "manifest unknown"}},
	}
	assert.True(t, IsImageNotFound(err))
}

func TestIsImageNotFound_WrappedTypedError(t *testing.T) {
	inner := &transport.Error{
		StatusCode: 404,
		Errors:     []transport.Diagnostic{{Code: transport.ManifestUnknownErrorCode}},
	}
	assert.True(t, IsImageNotFound(fmt.Errorf("get manifest: %w", inner)))
}

func TestIsImageNotFound_FallbackString(t *testing.T) {
	assert.True(t, IsImageNotFound(errors.New("MANIFEST_UNKNOWN: not found")))
	assert.True(t, IsImageNotFound(errors.New("404 Not Found")))
}

func TestIsImageNotFound_Negative(t *testing.T) {
	assert.False(t, IsImageNotFound(errors.New("some other error")))
	assert.False(t, IsImageNotFound(nil))
}

func TestIsRepoNotFound_TypedError(t *testing.T) {
	err := &transport.Error{
		StatusCode: 404,
		Errors:     []transport.Diagnostic{{Code: transport.NameUnknownErrorCode, Message: "repository name not known"}},
	}
	assert.True(t, IsRepoNotFound(err))
}

func TestIsRepoNotFound_WrappedTypedError(t *testing.T) {
	inner := &transport.Error{
		StatusCode: 404,
		Errors:     []transport.Diagnostic{{Code: transport.NameUnknownErrorCode}},
	}
	assert.True(t, IsRepoNotFound(fmt.Errorf("check repo: %w", inner)))
}

func TestIsRepoNotFound_FallbackString(t *testing.T) {
	assert.True(t, IsRepoNotFound(errors.New("NAME_UNKNOWN: repo")))
}

func TestIsRepoNotFound_Negative(t *testing.T) {
	assert.False(t, IsRepoNotFound(errors.New("some other error")))
	assert.False(t, IsRepoNotFound(nil))
}
