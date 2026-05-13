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

package imagefs

import "errors"

// ErrNotFound signals an unknown layer reference or a missing file.
var ErrNotFound = errors.New("not found")

// ErrNotRegularFile signals that a path exists but is not a regular file.
var ErrNotRegularFile = errors.New("not a regular file")

// ErrStopWalk stops a WalkTar iteration without surfacing as an error.
var ErrStopWalk = errors.New("stop walk")

// ErrFileTooLarge signals that a file's size exceeds the in-memory cap
// imposed by ReadFile to bound peak RSS when serving `fs cat`.
var ErrFileTooLarge = errors.New("file too large")
