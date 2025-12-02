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

package usecase

import "errors"

// Domain errors for mirror operations
// These errors decouple the usecase layer from infrastructure-specific errors

var (
	// ErrImageNotFound indicates that the requested image does not exist in the registry
	ErrImageNotFound = errors.New("image not found")

	// ErrRegistryUnauthorized indicates authentication failure
	ErrRegistryUnauthorized = errors.New("registry authentication failed")

	// ErrRegistryUnavailable indicates the registry is not accessible
	ErrRegistryUnavailable = errors.New("registry unavailable")

	// ErrInvalidTag indicates the tag format is invalid
	ErrInvalidTag = errors.New("invalid tag format")

	// ErrInvalidDigest indicates the digest format is invalid
	ErrInvalidDigest = errors.New("invalid digest format")
)

