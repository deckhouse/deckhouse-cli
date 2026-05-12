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

import (
	"archive/tar"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"sort"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// MergedFS returns the effective filesystem contents of img: layers applied
// bottom-up, whiteouts (single-file and opaque) honored. Entries are sorted
// by Path.
func MergedFS(img v1.Image) ([]Entry, error) {
	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("get layers: %w", err)
	}

	fileMap := make(map[string]Entry)
	for i, layer := range layers {
		rc, err := layer.Uncompressed()
		if err != nil {
			return nil, fmt.Errorf("uncompress layer %d: %w", i+1, err)
		}
		err = mergeLayer(rc, fileMap)
		_ = rc.Close()
		if err != nil {
			return nil, fmt.Errorf("layer %d: %w", i+1, err)
		}
	}
	return sortedEntries(fileMap), nil
}

// ReadFile returns the content of filePath in the merged FS. Layers are
// scanned top-down; a whiteout found before a matching entry causes
// ErrNotFound.
func ReadFile(img v1.Image, filePath string) ([]byte, error) {
	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("get layers: %w", err)
	}
	want := normalizePath(filePath)

	for i := len(layers) - 1; i >= 0; i-- {
		content, status, err := readFromLayer(layers[i], want)
		if err != nil {
			return nil, fmt.Errorf("layer %d: %w", i+1, err)
		}
		switch status {
		case readFound:
			return content, nil
		case readNonRegular:
			return nil, fmt.Errorf("%w: %s", ErrNotRegularFile, filePath)
		case readDeleted:
			return nil, fmt.Errorf("%w: %s", ErrNotFound, filePath)
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrNotFound, filePath)
}

// ---- internal helpers ----

// readStatus reflects the outcome of reading one file from one layer during
// merged-FS traversal.
type readStatus int

const (
	readMissing readStatus = iota
	readFound
	readNonRegular
	readDeleted
)

// maxReadFileSize caps the in-memory buffer used by readFromLayer so a
// `fs cat` on a multi-GB log file (or a tar bomb whose tar.Header.Size
// claims an absurd length) cannot OOM the CLI. 256 MiB is well above any
// realistic config / script / manifest, while staying within a typical
// CI runner's memory budget. Declared as var so tests can lower it.
var maxReadFileSize int64 = 256 << 20

// readFromLayer searches one layer for wantPath and decides its fate by
// OCI whiteout rules: a same-layer real entry for wantPath always wins,
// regardless of tar order versus a same-layer whiteout - whiteouts apply
// to LOWER layers, never to the layer they belong to. A whiteout on an
// ancestor of wantPath (regular or opaque) deletes wantPath as well.
//
// readStatus distinguishes:
//   - readMissing: path does not appear in this layer at all
//   - readFound: regular file located; content holds its bytes
//   - readNonRegular: path exists but is dir/symlink/hardlink (cat-incompatible)
//   - readDeleted: a whiteout marks the path (or an ancestor) as removed
func readFromLayer(layer v1.Layer, wantPath string) ([]byte, readStatus, error) {
	rc, err := layer.Uncompressed()
	if err != nil {
		return nil, readMissing, fmt.Errorf("uncompress: %w", err)
	}
	defer rc.Close()

	var (
		content    []byte
		realStatus readStatus
		hasReal    bool
		deleted    bool
	)
	err = WalkTar(rc, func(hdr *tar.Header, r io.Reader) error {
		name := normalizePath(hdr.Name)
		if target, opaque := Whiteout(name); target != "" || opaque {
			t := normalizePath(target)
			if t == wantPath || isAncestor(t, wantPath) {
				deleted = true
			}
			return nil
		}
		if name != wantPath {
			return nil
		}
		hasReal = true
		if !isRegularTarFile(hdr.Typeflag) {
			realStatus = readNonRegular
			content = nil
			return nil
		}
		b, rerr := io.ReadAll(io.LimitReader(r, maxReadFileSize+1))
		if rerr != nil {
			return rerr
		}
		if int64(len(b)) > maxReadFileSize {
			return fmt.Errorf("%w: %s (limit %d bytes)", ErrFileTooLarge, wantPath, maxReadFileSize)
		}
		content = b
		realStatus = readFound
		return nil
	})
	if err != nil {
		return nil, readMissing, err
	}
	if hasReal {
		return content, realStatus, nil
	}
	if deleted {
		return nil, readDeleted, nil
	}
	return nil, readMissing, nil
}

func isRegularTarFile(typeflag byte) bool {
	// archive/tar.Reader normalizes the legacy '\x00' (TypeRegA) typeflag
	// to TypeReg before our callback ever sees the header (see Go stdlib
	// archive/tar/reader.go: "Legacy archives use trailing slash for
	// directories"), so checking only TypeReg is sufficient.
	return typeflag == tar.TypeReg
}

// mergeLayer applies one layer to fileMap in two phases to respect whiteout
// semantics correctly:
//  1. Collect all real entries / whiteouts / opaque markers of this layer.
//  2. Whiteouts delete matching paths from prior layers (in fileMap) first.
//  3. Opaque markers clear descendants of the marked directory.
//  4. This layer's real entries are copied into fileMap last, so opaques in
//     this layer do not wipe its own additions.
func mergeLayer(rc io.Reader, fileMap map[string]Entry) error {
	thisLayer := make(map[string]Entry)
	whiteouts := make(map[string]struct{})
	opaques := make(map[string]struct{})

	if err := WalkTar(rc, func(hdr *tar.Header, _ io.Reader) error {
		name := normalizePath(hdr.Name)
		target, opaque := Whiteout(name)
		if opaque {
			opaques[normalizePath(target)] = struct{}{}
			return nil
		}
		if target != "" {
			whiteouts[normalizePath(target)] = struct{}{}
			return nil
		}
		thisLayer[name] = headerToEntry(hdr)
		return nil
	}); err != nil {
		return err
	}

	for wt := range whiteouts {
		delete(fileMap, wt)
		for k := range fileMap {
			if isAncestor(wt, k) {
				delete(fileMap, k)
			}
		}
	}
	for dir := range opaques {
		if dir == "." {
			for k := range fileMap {
				delete(fileMap, k)
			}
			continue
		}
		for k := range fileMap {
			if isAncestor(dir, k) {
				delete(fileMap, k)
			}
		}
	}
	maps.Copy(fileMap, thisLayer)
	return nil
}

func sortedEntries(m map[string]Entry) []Entry {
	out := make([]Entry, 0, len(m))
	for _, e := range m {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func headerToEntry(hdr *tar.Header) Entry {
	mode := fs.FileMode(hdr.Mode & 0o777)
	entryType := mapType(hdr.Typeflag)
	switch entryType {
	case TypeDir:
		mode |= fs.ModeDir
	case TypeSymlink:
		mode |= fs.ModeSymlink
	}
	return Entry{
		Path:     normalizePath(hdr.Name),
		Type:     entryType,
		Size:     hdr.Size,
		Mode:     mode,
		ModeStr:  mode.String(),
		Linkname: hdr.Linkname,
	}
}

func mapType(t byte) EntryType {
	switch t {
	case tar.TypeReg:
		return TypeFile
	case tar.TypeDir:
		return TypeDir
	case tar.TypeSymlink:
		return TypeSymlink
	case tar.TypeLink:
		return TypeHardlink
	default:
		return TypeOther
	}
}
