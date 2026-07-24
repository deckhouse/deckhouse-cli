//go:build !linux && !windows && !darwin && !freebsd && !openbsd

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

package archive

import (
	"os"
)

func openArchiveLockAnchor(_ *RootedSource) (*os.File, error) {
	return nil, unsupportedRootedArchivePlatform("archive namespace lock")
}

func openArchiveLockAt(_ *os.File, _, path string) (*os.File, error) {
	return nil, unsupportedRootedArchivePlatform(path)
}

func tryArchiveAnchorLock(_ *os.File, _ bool) (bool, error) {
	return false, unsupportedRootedArchivePlatform("archive namespace lock")
}

func tryArchiveRootLock(_ *os.File, _ bool) (bool, error) {
	return false, unsupportedRootedArchivePlatform("archive root lock")
}

func tryArchiveFileLock(_ *os.File, _ bool) (bool, error) {
	return false, unsupportedRootedArchivePlatform("archive lock entry")
}

func unlockArchiveAnchorLock(_ *os.File) error {
	return nil
}

func closeArchiveLockAnchor(_ *os.File) error {
	return nil
}

func unlockArchiveFileLock(_ *os.File) error {
	return nil
}

func unlockArchiveRootLock(_ *os.File) error {
	return nil
}
