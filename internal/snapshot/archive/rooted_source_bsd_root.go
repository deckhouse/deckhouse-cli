//go:build darwin || freebsd || openbsd

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
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openArchiveRootUnix(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, classifyArchiveOpenError(path, true, err)
	}

	dir := os.NewFile(uintptr(fd), path)
	if dir == nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("open archive root %s: invalid directory descriptor", path)
	}

	return dir, nil
}
