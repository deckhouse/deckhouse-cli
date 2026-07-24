//go:build linux || darwin || freebsd || openbsd

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

func readDurableSymlinkAt(parent *os.File, name, path string) (string, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return "", classifyArchiveOpenError(path, false, err)
	}

	if stat.Mode&unix.S_IFMT != unix.S_IFLNK {
		return "", fmt.Errorf("%s is not a symbolic link: %w", path, ErrNonRegularArchiveArtifact)
	}

	buffer := make([]byte, 256)
	for len(buffer) <= maxRootedPathBytes {
		length, err := unix.Readlinkat(int(parent.Fd()), name, buffer)
		if err != nil {
			return "", fmt.Errorf("read durable symbolic link %s: %w", path, err)
		}

		if length < len(buffer) {
			return string(buffer[:length]), nil
		}

		buffer = make([]byte, len(buffer)*2)
	}

	return "", fmt.Errorf("durable symbolic link %s exceeds %d bytes: %w",
		path, maxRootedPathBytes, ErrNonRegularArchiveArtifact)
}
