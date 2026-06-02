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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const (
	// indexFileName is the OCI image index file name. Multiple bundle archives may
	// carry the same layout path (e.g. packages/<name>/version) while exposing
	// different tag subsets, so colliding index.json files must be merged rather
	// than overwritten when unpacking into a shared directory.
	indexFileName = "index.json"

	refNameAnnotation = "org.opencontainers.image.ref.name"
)

func Unpack(ctx context.Context, source io.Reader, targetPath string, pkgName string) error {
	var err error

	tarReader := tar.NewReader(source)

	// support old behavior when inside "module-<name>.tar" isn't have modules/<name> folder
	// if unpack this archive without check, it will be pushed to registry root, not to modules/<name>
	// isLegacyModule is true by default if we unpacking module-<name>.tar
	isLegacyModule := strings.HasPrefix(pkgName, "module-")
	moduleName := strings.TrimPrefix(pkgName, "module-")

	for {
		if err = ctx.Err(); err != nil {
			return err
		}

		tarHdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if tarHdr.Typeflag != tar.TypeReg {
			continue
		}

		if isLegacyModule && strings.HasPrefix(tarHdr.Name, "modules/"+moduleName) {
			isLegacyModule = false
		}

		writePath := filepath.Join(targetPath, "tmp", filepath.Clean(tarHdr.Name))
		if err = os.MkdirAll(filepath.Dir(writePath), 0o755); err != nil {
			return fmt.Errorf("setup dir tree: %w", err)
		}

		bundleFile, err := os.OpenFile(writePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
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

	from := filepath.Join(targetPath, "tmp")

	to := targetPath
	if isLegacyModule {
		to = filepath.Join(targetPath, "modules", moduleName)

		if err = os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
			return fmt.Errorf("setup dir tree: %w", err)
		}

		err = moveFiles(from, to)
		if err != nil {
			return fmt.Errorf("move module from tmp: %w", err)
		}

		return nil
	}

	if err = os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		return fmt.Errorf("setup dir tree: %w", err)
	}

	err = moveFiles(from, to)
	if err != nil {
		return fmt.Errorf("move module from tmp to '%s': %w", to, err)
	}

	_ = os.RemoveAll(filepath.Join(targetPath, "tmp"))

	return nil
}

func Pack(ctx context.Context, sourcePath string, sink io.Writer) error {
	return PackWithPrefix(ctx, sourcePath, "", sink)
}

// PackWithPrefix packs directory contents into tar with an optional prefix for all paths.
// For example, PackWithPrefix(ctx, "/tmp/module", "modules/stronghold", sink) will create
// tar entries like "modules/stronghold/index.json" instead of just "index.json".
func PackWithPrefix(ctx context.Context, sourcePath string, prefix string, sink io.Writer) error {
	return PackSourcesWithPrefix(ctx, sink, PackSource{Dir: sourcePath, Prefix: prefix})
}

// PackSource describes one directory to be packed into a tar stream.
type PackSource struct {
	// Dir is the source directory on disk whose contents are packed.
	Dir string
	// Prefix is prepended to every entry's path inside the tar.
	Prefix string
	// ExcludeDirs lists directories (by path) to skip together with their
	// descendants. It is used to keep a nested OCI layout (e.g. a package's
	// version/ sub-layout) out of an archive that is produced separately.
	ExcludeDirs []string
}

// PackSourcesWithPrefix packs several (dir, prefix) sources into a single tar
// stream written to sink. It is the multi-source counterpart of
// PackWithPrefix and is used to aggregate layouts from many packages into one
// archive (e.g. package-versions.tar).
func PackSourcesWithPrefix(ctx context.Context, sink io.Writer, sources ...PackSource) error {
	tarWriter := tar.NewWriter(sink)

	for _, src := range sources {
		walkFn := packFuncWithPrefix(ctx, src.Dir, src.Prefix, src.ExcludeDirs, tarWriter)
		if err := filepath.Walk(src.Dir, walkFn); err != nil {
			return fmt.Errorf("pack mirrored images into tar: %w", err)
		}
	}

	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("write tar trailer: %w", err)
	}

	return nil
}

func packFuncWithPrefix(ctx context.Context, pathPrefix string, tarPrefix string, excludeDirs []string, writer *tar.Writer) filepath.WalkFunc {
	unixEpochStart := time.Unix(0, 0)

	excluded := make(map[string]struct{}, len(excludeDirs))
	for _, d := range excludeDirs {
		excluded[filepath.Clean(d)] = struct{}{}
	}

	return func(path string, info fs.FileInfo, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			return err
		}

		if info.IsDir() {
			if _, skip := excluded[filepath.Clean(path)]; skip {
				return filepath.SkipDir
			}
		}

		if path == pathPrefix || info.IsDir() {
			return nil
		}

		blobFile, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("read file: %w", err)
		}

		pathInTar := strings.TrimPrefix(path, pathPrefix+string(os.PathSeparator))
		// Add prefix if specified
		if tarPrefix != "" {
			pathInTar = tarPrefix + "/" + pathInTar
		}

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

func moveFiles(from, to string) error {
	files, err := os.ReadDir(from)
	if err != nil {
		return fmt.Errorf("read directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			if err = os.MkdirAll(filepath.Join(to, file.Name()), 0o755); err != nil {
				return fmt.Errorf("setup dir tree: %w", err)
			}

			if err = moveFiles(filepath.Join(from, file.Name()), filepath.Join(to, file.Name())); err != nil {
				return fmt.Errorf("move files: %w", err)
			}

			continue
		}

		fromPath := filepath.Join(from, file.Name())
		toPath := filepath.Join(to, file.Name())

		// Two archives can target the same OCI layout: they share blobs (named by
		// unique content hash, so no collision) but each carries its own index.json.
		// A plain rename would let the last archive's index overwrite the previous
		// one, dropping the tags it didn't include even though their blobs are
		// already on disk. Merge the manifests instead to keep every tag.
		if file.Name() == indexFileName {
			if _, statErr := os.Stat(toPath); statErr == nil {
				if err = mergeIndexJSON(fromPath, toPath); err != nil {
					return fmt.Errorf("merge index.json: %w", err)
				}

				if err = os.Remove(fromPath); err != nil {
					return fmt.Errorf("remove merged source index: %w", err)
				}

				continue
			} else if !errors.Is(statErr, fs.ErrNotExist) {
				return fmt.Errorf("stat destination index: %w", statErr)
			}
		}

		err = os.Rename(fromPath, toPath)
		if err != nil {
			return fmt.Errorf("move file: %w", err)
		}
	}

	return nil
}

// ociIndex is a minimal representation of an OCI image index (index.json)
// sufficient to merge manifest lists from multiple archives.
type ociIndex struct {
	SchemaVersion int64             `json:"schemaVersion"`
	MediaType     types.MediaType   `json:"mediaType,omitempty"`
	Manifests     []v1.Descriptor   `json:"manifests"`
	Annotations   map[string]string `json:"annotations,omitempty"`
	Subject       *v1.Descriptor    `json:"subject,omitempty"`
}

// mergeIndexJSON merges the OCI image index at srcPath into the one at dstPath,
// writing the union back to dstPath. Manifests are deduplicated by digest paired
// with their ref.name annotation so layouts that share blobs but expose different
// tag subsets don't lose tags when unpacked into the same directory.
func mergeIndexJSON(srcPath, dstPath string) error {
	srcIndex, err := readOCIIndex(srcPath)
	if err != nil {
		return fmt.Errorf("read source index %q: %w", srcPath, err)
	}

	dstIndex, err := readOCIIndex(dstPath)
	if err != nil {
		return fmt.Errorf("read destination index %q: %w", dstPath, err)
	}

	if dstIndex.SchemaVersion == 0 {
		dstIndex.SchemaVersion = srcIndex.SchemaVersion
	}

	if dstIndex.MediaType == "" {
		dstIndex.MediaType = srcIndex.MediaType
	}

	manifestKey := func(d v1.Descriptor) string {
		return d.Digest.String() + "\x00" + d.Annotations[refNameAnnotation]
	}

	seen := make(map[string]struct{}, len(dstIndex.Manifests)+len(srcIndex.Manifests))
	merged := make([]v1.Descriptor, 0, len(dstIndex.Manifests)+len(srcIndex.Manifests))

	for _, list := range [][]v1.Descriptor{dstIndex.Manifests, srcIndex.Manifests} {
		for _, descriptor := range list {
			key := manifestKey(descriptor)
			if _, ok := seen[key]; ok {
				continue
			}

			seen[key] = struct{}{}

			merged = append(merged, descriptor)
		}
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Annotations[refNameAnnotation] < merged[j].Annotations[refNameAnnotation]
	})

	dstIndex.Manifests = merged

	raw, err := json.MarshalIndent(dstIndex, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal merged index: %w", err)
	}

	if err = os.WriteFile(dstPath, raw, 0o600); err != nil {
		return fmt.Errorf("write merged index %q: %w", dstPath, err)
	}

	return nil
}

func readOCIIndex(path string) (*ociIndex, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	index := &ociIndex{}
	if err = json.Unmarshal(raw, index); err != nil {
		return nil, fmt.Errorf("parse index: %w", err)
	}

	return index, nil
}
