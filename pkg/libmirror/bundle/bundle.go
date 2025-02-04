/*
Copyright 2024 Flant JSC

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

package bundle

import (
	"archive/tar"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func Unpack(ctx context.Context, source io.Reader, targetPath string) error {
	var err error
	tarReader := tar.NewReader(source)

	for {
		if err = ctx.Err(); err != nil {
			return err
		}

		tarHdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		writePath := filepath.Join(targetPath, filepath.Clean(tarHdr.Name))
		if err = os.MkdirAll(filepath.Dir(writePath), 0o755); err != nil {
			return fmt.Errorf("setup dir tree: %w", err)
		}
		bundleFile, err := os.OpenFile(writePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return fmt.Errorf("create file: %w", err)
		}
		if _, err = io.Copy(bundleFile, tarReader); err != nil {
			return fmt.Errorf("write %q: %w", writePath, err)
		}
		if err = bundleFile.Sync(); err != nil {
			return fmt.Errorf("write %q: %w", writePath, err)
		}
		if err = bundleFile.Close(); err != nil {
			return fmt.Errorf("write %q: %w", writePath, err)
		}
	}

	return nil
}

func Pack(ctx context.Context, sourcePath string, sink io.Writer) error {
	tarWriter := tar.NewWriter(sink)
	if err := filepath.Walk(sourcePath, packFunc(ctx, sourcePath, tarWriter)); err != nil {
		return fmt.Errorf("pack mirrored images into tar: %w", err)
	}

	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("write tar trailer: %w", err)
	}

	return nil
}

func packFunc(ctx context.Context, pathPrefix string, writer *tar.Writer) filepath.WalkFunc {
	unixEpochStart := time.Unix(0, 0)
	return func(path string, info fs.FileInfo, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return err
		}
		if path == pathPrefix || info.IsDir() {
			return nil
		}

		blobFile, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("read file: %w", err)
		}

		pathInTar := strings.TrimPrefix(path, pathPrefix+string(os.PathSeparator))
		err = writer.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Format:   tar.FormatGNU,
			Name:     filepath.ToSlash(pathInTar),
			Size:     info.Size(),
			Mode:     0777,
			ModTime:  unixEpochStart,
		})
		if err != nil {
			return fmt.Errorf("write tar header: %w", err)
		}

		if _, err = bufio.NewReaderSize(blobFile, 512*1024).WriteTo(writer); err != nil {
			return fmt.Errorf("write file to tar: %w", err)
		}

		if err = blobFile.Close(); err != nil {
			return fmt.Errorf("close file descriptor: %w", err)
		}

		// We don't care about error here.
		// Whole folder with unpacked images will be deleted after bundle is packed.
		//
		// We attempt to delete packed parts of layout here only to save some storage space,
		// avoiding duplication of data that was already written to tar bundle.
		_ = os.Remove(path)

		return nil
	}
}
