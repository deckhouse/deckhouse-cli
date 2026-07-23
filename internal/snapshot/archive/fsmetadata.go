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
	"archive/tar"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// PAXFSCodec identifies the codec applied to one regular filesystem entry.
	PAXFSCodec = "D8.snapshot.fs.codec"
	// PAXFSOriginalPath preserves the source path independently of the stored codec suffix.
	PAXFSOriginalPath = "D8.snapshot.fs.originalPath"
	// PAXFSRawSize records the exact plaintext byte count before compression.
	PAXFSRawSize = "D8.snapshot.fs.rawSize"
)

// ErrInvalidFSMetadata is returned when a regular filesystem tar entry does not
// carry a complete, internally consistent set of d8 PAX metadata.
var ErrInvalidFSMetadata = errors.New("invalid filesystem tar metadata")

var fsCodecExtensions = map[string]string{
	"gzip": ".gz",
	"lz4":  ".lz4",
	"none": "",
	"zstd": ".zst",
}

// FSMetadata is the checksum-covered format contract for one regular data.tar entry.
type FSMetadata struct {
	Codec        string
	OriginalPath string
	RawSize      int64
}

// NewFSMetadata validates and constructs metadata for a new regular entry.
func NewFSMetadata(codec, originalPath string, rawSize int64) (FSMetadata, error) {
	metadata := FSMetadata{
		Codec:        codec,
		OriginalPath: originalPath,
		RawSize:      rawSize,
	}

	if err := metadata.validate(); err != nil {
		return FSMetadata{}, err
	}

	return metadata, nil
}

// ParseFSMetadata strictly decodes the required PAX records from hdr and verifies
// that hdr.Name is the canonical stored path for the declared codec.
func ParseFSMetadata(hdr *tar.Header) (FSMetadata, error) {
	if hdr == nil {
		return FSMetadata{}, fmt.Errorf("%w: nil tar header", ErrInvalidFSMetadata)
	}

	if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != 0 {
		return FSMetadata{}, fmt.Errorf("%w: entry %q is not regular", ErrInvalidFSMetadata, hdr.Name)
	}

	codec, ok := hdr.PAXRecords[PAXFSCodec]
	if !ok {
		return FSMetadata{}, fmt.Errorf("%w: entry %q is missing PAX key %q", ErrInvalidFSMetadata, hdr.Name, PAXFSCodec)
	}

	originalPath, ok := hdr.PAXRecords[PAXFSOriginalPath]
	if !ok {
		return FSMetadata{}, fmt.Errorf("%w: entry %q is missing PAX key %q", ErrInvalidFSMetadata, hdr.Name, PAXFSOriginalPath)
	}

	rawSizeValue, ok := hdr.PAXRecords[PAXFSRawSize]
	if !ok {
		return FSMetadata{}, fmt.Errorf("%w: entry %q is missing PAX key %q", ErrInvalidFSMetadata, hdr.Name, PAXFSRawSize)
	}

	rawSize, err := strconv.ParseInt(rawSizeValue, 10, 64)
	if err != nil || rawSize < 0 || strconv.FormatInt(rawSize, 10) != rawSizeValue {
		return FSMetadata{}, fmt.Errorf("%w: entry %q has non-canonical %q value %q",
			ErrInvalidFSMetadata, hdr.Name, PAXFSRawSize, rawSizeValue)
	}

	metadata, err := NewFSMetadata(codec, originalPath, rawSize)
	if err != nil {
		return FSMetadata{}, fmt.Errorf("parse metadata for entry %q: %w", hdr.Name, err)
	}

	storedPath, err := metadata.StoredPath()
	if err != nil {
		return FSMetadata{}, err
	}

	if hdr.Name != storedPath {
		return FSMetadata{}, fmt.Errorf("%w: stored path %q does not match codec %q and original path %q (want %q)",
			ErrInvalidFSMetadata, hdr.Name, metadata.Codec, metadata.OriginalPath, storedPath)
	}

	if metadata.Codec == "none" && hdr.Size != metadata.RawSize {
		return FSMetadata{}, fmt.Errorf("%w: uncompressed entry %q stores %d bytes but declares raw size %d",
			ErrInvalidFSMetadata, hdr.Name, hdr.Size, metadata.RawSize)
	}

	return metadata, nil
}

// PAXRecords returns a fresh map containing the three required records.
func (m FSMetadata) PAXRecords() map[string]string {
	return map[string]string{
		PAXFSCodec:        m.Codec,
		PAXFSOriginalPath: m.OriginalPath,
		PAXFSRawSize:      strconv.FormatInt(m.RawSize, 10),
	}
}

// StoredPath returns the canonical tar member name for this metadata.
func (m FSMetadata) StoredPath() (string, error) {
	ext, err := FSCodecExtension(m.Codec)
	if err != nil {
		return "", err
	}

	return m.OriginalPath + ext, nil
}

// FSCodecExtension maps a validated filesystem codec name to its stored suffix.
func FSCodecExtension(codec string) (string, error) {
	ext, ok := fsCodecExtensions[codec]
	if !ok {
		return "", fmt.Errorf("%w: unsupported codec %q", ErrInvalidFSMetadata, codec)
	}

	return ext, nil
}

func (m FSMetadata) validate() error {
	if _, err := FSCodecExtension(m.Codec); err != nil {
		return err
	}

	if m.RawSize < 0 {
		return fmt.Errorf("%w: negative raw size %d", ErrInvalidFSMetadata, m.RawSize)
	}

	if err := validateFSOriginalPath(m.OriginalPath); err != nil {
		return err
	}

	return nil
}

func validateFSOriginalPath(originalPath string) error {
	if originalPath == "" {
		return fmt.Errorf("%w: original path is empty", ErrInvalidFSMetadata)
	}

	if strings.HasPrefix(originalPath, "/") || strings.ContainsRune(originalPath, '\\') {
		return fmt.Errorf("%w: original path %q is not a portable relative path", ErrInvalidFSMetadata, originalPath)
	}

	for _, r := range originalPath {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: original path %q contains a control byte", ErrInvalidFSMetadata, originalPath)
		}
	}

	segments := strings.Split(originalPath, "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("%w: original path %q contains an unsafe element", ErrInvalidFSMetadata, originalPath)
		}
	}

	if segments[0] == FSMetaDirName {
		return fmt.Errorf("%w: original path %q enters reserved namespace %q",
			ErrInvalidFSMetadata, originalPath, FSMetaDirName)
	}

	return nil
}
