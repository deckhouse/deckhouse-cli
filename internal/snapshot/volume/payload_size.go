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

package volume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

// Payload mode strings recorded in PayloadSize.Mode, matching the sibling
// snapimport.dataImportPayloadBlock/dataImportPayloadFilesystem naming.
const (
	payloadModeBlock      = "block"
	payloadModeFilesystem = "filesystem"
)

// PayloadSize is the measured byte footprint of a snapshot node's captured volume
// payload, as it actually exists on disk — as opposed to VolumeInfo.Size, which is the
// nominal PVC quantity used to provision a scratch volume on re-import.
type PayloadSize struct {
	// RawBytes is the exact decoded (plaintext) byte length of the payload: for a Block
	// volume, data.bin[.<ext>]'s decoded length; for a Filesystem volume, the sum of
	// data.tar's regular-entry raw sizes.
	RawBytes int64
	// StoredBytes is the on-disk byte length of the payload artifact itself
	// (data.bin[.<ext>] or data.tar).
	StoredBytes int64
	// Mode is payloadModeBlock or payloadModeFilesystem for a node that carries a volume
	// payload, "" for a node with no payload (aggregator node).
	Mode string
}

// MeasurePayload determines the exact raw and stored byte sizes of a snapshot node's
// captured payload by reading it from disk: for a Block volume, it decodes (or, for
// zstd, reads the frame headers of) data.bin[.<ext>]; for a Filesystem volume, it sums
// data.tar's regular-entry raw sizes and stats the tar file itself for StoredBytes.
//
// destination is nil for a plain-filesystem archive and non-nil for one opened through a
// locked rooted view (see archive.RootedDestination); both read the identical on-disk
// layout, only through different I/O primitives. Returns a zero PayloadSize with
// Mode == "" for a node with no payload (aggregator node) — not an error.
func MeasurePayload(ctx context.Context, destination *archive.RootedDestination, nodeDir string) (PayloadSize, error) {
	blockPayload, hasBlock, err := classifyBlockPayload(destination, nodeDir)
	if err != nil {
		return PayloadSize{}, fmt.Errorf("classify block payload in %s: %w", nodeDir, err)
	}

	if hasBlock {
		size, measureErr := measureBlockPayload(ctx, destination, blockPayload)
		if measureErr != nil {
			return PayloadSize{}, fmt.Errorf("measure block payload %s: %w", blockPayload.Path, measureErr)
		}

		return size, nil
	}

	tarPath := filepath.Join(nodeDir, archive.FsTarName)

	hasTar, err := payloadFileExists(destination, tarPath)
	if err != nil {
		return PayloadSize{}, fmt.Errorf("inspect %s: %w", tarPath, err)
	}

	if !hasTar {
		return PayloadSize{}, nil
	}

	size, err := measureFSPayload(ctx, destination, tarPath)
	if err != nil {
		return PayloadSize{}, fmt.Errorf("measure filesystem payload %s: %w", tarPath, err)
	}

	return size, nil
}

// classifyBlockPayload resolves nodeDir's block payload (see archive.ClassifyBlockPayload),
// through destination's locked view when set.
func classifyBlockPayload(destination *archive.RootedDestination, nodeDir string) (archive.BlockPayload, bool, error) {
	if destination == nil {
		return archive.ClassifyBlockPayload(nodeDir)
	}

	return destination.FindBlockData(nodeDir)
}

// payloadFileExists reports whether path exists, through destination's locked view when set.
func payloadFileExists(destination *archive.RootedDestination, path string) (bool, error) {
	var err error

	if destination == nil {
		_, err = os.Stat(path)
	} else {
		_, err = destination.Stat(path)
	}

	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err
}

// openPayloadFile opens path for reading, through destination's locked view when set.
func openPayloadFile(destination *archive.RootedDestination, path string) (*os.File, error) {
	if destination == nil {
		return os.Open(path)
	}

	return destination.OpenRegular(path)
}

// measureBlockPayload opens and measures one node's block-volume payload file.
func measureBlockPayload(
	ctx context.Context,
	destination *archive.RootedDestination,
	payload archive.BlockPayload,
) (PayloadSize, error) {
	file, err := openPayloadFile(destination, payload.Path)
	if err != nil {
		return PayloadSize{}, err
	}

	size, measureErr := measureOpenBlockPayload(ctx, payload.Ext, file)
	closeErr := file.Close()

	if err := errors.Join(measureErr, closeErr); err != nil {
		return PayloadSize{}, err
	}

	return size, nil
}

// measureOpenBlockPayload measures an already-open block payload file. ext is the
// classified codec extension (see archive.BlockPayload.Ext) — callers MUST pass that
// value rather than re-deriving it from the filename, for the same reason
// archive.BlockPayload.Ext's doc comment gives.
func measureOpenBlockPayload(ctx context.Context, ext string, file *os.File) (PayloadSize, error) {
	info, err := file.Stat()
	if err != nil {
		return PayloadSize{}, fmt.Errorf("stat block payload: %w", err)
	}

	section := io.NewSectionReader(file, 0, info.Size())

	rawBytes, err := compress.DecodedSize(ctx, ext, section)
	if err != nil {
		return PayloadSize{}, fmt.Errorf("decode block payload size: %w", err)
	}

	return PayloadSize{
		RawBytes:    rawBytes,
		StoredBytes: info.Size(),
		Mode:        payloadModeBlock,
	}, nil
}

// measureFSPayload opens and measures one node's filesystem-volume payload file.
func measureFSPayload(
	ctx context.Context,
	destination *archive.RootedDestination,
	tarPath string,
) (PayloadSize, error) {
	file, err := openPayloadFile(destination, tarPath)
	if err != nil {
		return PayloadSize{}, err
	}

	size, measureErr := measureOpenFSPayload(ctx, file)
	closeErr := file.Close()

	if err := errors.Join(measureErr, closeErr); err != nil {
		return PayloadSize{}, err
	}

	return size, nil
}

// measureOpenFSPayload measures an already-open data.tar file: the container itself is
// never compressed as a whole (only individual entries are, per-entry), so its own
// on-disk length is StoredBytes and RawBytes is the sum of every entry's decoded size.
func measureOpenFSPayload(ctx context.Context, file *os.File) (PayloadSize, error) {
	info, err := file.Stat()
	if err != nil {
		return PayloadSize{}, fmt.Errorf("stat filesystem payload: %w", err)
	}

	section := io.NewSectionReader(file, 0, info.Size())

	rawBytes, err := archive.SumTarRawSizes(ctx, section)
	if err != nil {
		return PayloadSize{}, fmt.Errorf("sum filesystem payload raw sizes: %w", err)
	}

	return PayloadSize{
		RawBytes:    rawBytes,
		StoredBytes: info.Size(),
		Mode:        payloadModeFilesystem,
	}, nil
}
