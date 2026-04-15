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
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// WalkTar reads a tar stream and invokes fn for each entry. Returning
// ErrStopWalk stops iteration without propagating as an error.
//
// The function is the shared tar-traversal primitive used by all readers
// and the extractor. Callers pass the io.Reader obtained from
// v1.Layer.Uncompressed().
func WalkTar(rc io.Reader, fn func(*tar.Header, io.Reader) error) error {
	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("tar next: %w", err)
		}
		if err := fn(hdr, tr); err != nil {
			if errors.Is(err, ErrStopWalk) {
				return nil
			}
			return err
		}
	}
}

// normalizePath strips leading "./" and "/", trailing "/", and cleans the
// result. Empty paths collapse to ".".
func normalizePath(p string) string {
	p = strings.TrimPrefix(p, "./")
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return "."
	}
	return path.Clean(p)
}

// isAncestor reports whether descendant lives under ancestor (not equal).
func isAncestor(ancestor, descendant string) bool {
	if ancestor == "." {
		return descendant != "."
	}
	return strings.HasPrefix(descendant, ancestor+"/")
}
