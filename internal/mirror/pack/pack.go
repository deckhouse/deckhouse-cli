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

// Package pack contains the atomic-write helper shared by every component
// that emits a tar artifact into the user's bundle directory (platform,
// installer, security, per-module). Centralizing the staging/rename/cleanup
// logic here makes it impossible for one of those components to forget the
// pattern and accidentally leave a half-written or empty tar behind after a
// failed or cancelled pull - which is what produced the empty
// (~5 KiB) module-*.tar files that survived an interrupted mirror pull.
package pack

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
)

// Bundle stages the result of pack into a temporary file (or a set of
// temporary chunk files when bundleChunkSize > 0) and only promotes it to its
// final name on success. On any error - including ctx cancellation - the
// staged files are deleted so the bundle directory never contains stub
// artifacts for downloads that did not actually complete.
//
// pack must write the full tar payload to the io.Writer; it must not retain
// the writer past return.
func Bundle(
	ctx context.Context,
	bundleDir, pkgName string,
	bundleChunkSize int64,
	pack func(io.Writer) error,
) error {
	if bundleChunkSize > 0 {
		return bundleChunked(ctx, bundleDir, pkgName, bundleChunkSize, pack)
	}

	return bundleSingle(ctx, bundleDir, pkgName, pack)
}

func bundleChunked(
	ctx context.Context,
	bundleDir, pkgName string,
	bundleChunkSize int64,
	pack func(io.Writer) error,
) error {
	cw := chunked.NewChunkedFileWriter(bundleChunkSize, bundleDir, pkgName)

	if err := pack(cw); err != nil {
		cw.Cleanup()
		return fmt.Errorf("pack %s: %w", pkgName, err)
	}

	if err := cw.Close(); err != nil {
		cw.Cleanup()
		return fmt.Errorf("close %s: %w", pkgName, err)
	}
	// Respect cancellation just before publishing: don't rename a
	// half-aborted set of chunks into their final names.
	if cerr := ctx.Err(); cerr != nil {
		cw.Cleanup()
		return cerr
	}

	if err := cw.Finalize(); err != nil {
		return fmt.Errorf("finalize %s: %w", pkgName, err)
	}

	return nil
}

func bundleSingle(
	ctx context.Context,
	bundleDir, pkgName string,
	pack func(io.Writer) error,
) error {
	finalPath := filepath.Join(bundleDir, pkgName)
	tmpPath := finalPath + ".tmp"

	// Remove any leftover from a previous aborted run before creating fresh.
	_ = os.Remove(tmpPath)

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", pkgName, err)
	}

	if err = pack(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)

		return fmt.Errorf("pack %s: %w", pkgName, err)
	}

	if err = f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close %s: %w", pkgName, err)
	}

	if cerr := ctx.Err(); cerr != nil {
		_ = os.Remove(tmpPath)
		return cerr
	}

	if err = os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename %s: %w", pkgName, err)
	}

	return nil
}
