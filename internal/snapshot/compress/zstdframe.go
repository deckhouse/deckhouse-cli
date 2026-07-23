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

package compress

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// ErrCorruptZstdFrame is returned (wrapped) by SkipZstdFrames when the stream
// deviates from the subset of RFC 8878 this walker accepts: a non-Zstandard or
// skippable magic, a reserved/unused frame-header bit, a reserved block type, a
// malformed header shape, an arithmetic overflow, or any truncation. Compare
// with errors.Is.
var ErrCorruptZstdFrame = errors.New("corrupt zstd frame")

// RFC 8878 magic numbers. zstdFrameMagic is the little-endian value of the four
// leading bytes 28 B5 2F FD. Skippable frames use any magic in the inclusive
// range [0x184D2A50, 0x184D2A5F]; this walker rejects them — every frame in a
// data.bin.zst stream is a standard Zstandard frame produced by Codec.EncodeFrame.
const (
	zstdFrameMagic    uint32 = 0xFD2FB528
	skippableMagicMin uint32 = 0x184D2A50
	skippableMagicMax uint32 = 0x184D2A5F
)

// Fixed field widths, in bytes, of the parts this walker reads with io.ReadFull.
// Block payloads are never read — they are skipped with Seek — so no payload
// width appears here.
const (
	magicLen       = 4
	fhdLen         = 1
	windowLen      = 1
	blockHeaderLen = 3
	checksumLen    = 4
)

// Zstandard block types (RFC 8878 §3.1.1.2.2). The reserved type (3) must never
// appear and is rejected.
const (
	blockTypeRaw        uint32 = 0
	blockTypeRLE        uint32 = 1
	blockTypeCompressed uint32 = 2
)

// SkipZstdFrames returns the absolute byte offset, relative to the ReadSeeker's
// current position, at which frame n begins in a concatenation of independent
// Zstandard frames — i.e. the sum of the physical lengths of frames 0..n-1.
//
// It walks the stream by reading only frame/block headers (and the optional
// content checksum) into small fixed buffers with io.ReadFull, and skipping
// every block payload with Seek. It never wraps the reader in a buffered reader
// and never decodes payloads, so the reader's offset stays exactly aligned with
// the walker's own accounting.
//
// Because seeking past EOF is NOT an error for an io.ReadSeeker, existence of a
// byte range cannot be inferred from a successful Seek. SkipZstdFrames therefore
// establishes the real end bound up front (Seek to io.SeekEnd) and checks that
// enough bytes remain before every header read and every payload skip; a short
// stream fails with ErrCorruptZstdFrame rather than silently seeking into the void.
//
// Before returning, it validates the magic and complete header of the target
// frame n (so a returned offset always points at a well-formed frame start),
// then restores the reader to the position it held on entry — the call is
// non-destructive, including for n == 0. n must be a valid frame index in
// [0, frameCount); asking for n == frameCount fails because frame n's magic
// cannot be read. A negative n is an argument error and does not wrap
// ErrCorruptZstdFrame.
func SkipZstdFrames(rs io.ReadSeeker, n int) (int64, error) {
	if n < 0 {
		return 0, fmt.Errorf("skip zstd frames: negative frame count %d", n)
	}

	start, err := rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("skip zstd frames: querying current offset: %w", err)
	}

	end, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("skip zstd frames: querying end offset: %w", err)
	}

	if _, err := rs.Seek(start, io.SeekStart); err != nil {
		return 0, fmt.Errorf("skip zstd frames: restoring offset: %w", err)
	}

	w := &frameWalker{rs: rs, end: end, pos: start}

	for i := range n {
		if err := w.walkFrame(); err != nil {
			return 0, fmt.Errorf("skip zstd frames: walking frame %d: %w", i, err)
		}
	}

	target := w.pos

	// Existence is not validity: confirm the target frame's magic + header are
	// well-formed before handing its offset back as a boundary.
	if _, err := w.readMagicAndHeader(); err != nil {
		return 0, fmt.Errorf("skip zstd frames: validating target frame %d: %w", n, err)
	}

	if _, err := rs.Seek(start, io.SeekStart); err != nil {
		return 0, fmt.Errorf("skip zstd frames: restoring offset after validation: %w", err)
	}

	return target, nil
}

// frameWalker carries the shared state for a single SkipZstdFrames call: the
// reader, the real end bound (EOF), the running position, and a fixed 8-byte
// scratch buffer large enough for the widest field read (an 8-byte Frame_Content_Size).
type frameWalker struct {
	rs  io.ReadSeeker
	end int64
	pos int64
	buf [8]byte
}

// frameHeader holds the only per-frame header fact the walker needs after the
// header is consumed: whether a 4-byte content checksum trails the last block.
type frameHeader struct {
	hasChecksum bool
}

// ensure reports whether need bytes are available from the current position
// without running past the end bound, checking the pos+need arithmetic for
// overflow so a crafted length can never wrap into a passing range.
func (w *frameWalker) ensure(need int64) error {
	limit, err := addChecked(w.pos, need)
	if err != nil {
		return err
	}

	if limit > w.end {
		return fmt.Errorf("%w: need %d bytes at offset %d but stream ends at %d", ErrCorruptZstdFrame, need, w.pos, w.end)
	}

	return nil
}

// read consumes exactly n header bytes into the scratch buffer and returns the
// filled slice, valid only until the next read. Truncation surfaces as
// ErrCorruptZstdFrame both via the up-front bound check and via io.ReadFull.
func (w *frameWalker) read(n int) ([]byte, error) {
	if err := w.ensure(int64(n)); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(w.rs, w.buf[:n]); err != nil {
		return nil, fmt.Errorf("%w: reading %d header bytes at offset %d: %w", ErrCorruptZstdFrame, n, w.pos, err)
	}

	w.pos += int64(n)

	return w.buf[:n], nil
}

// skip advances past n payload bytes with Seek, never reading them. The bound
// check is mandatory: Seek past EOF succeeds, so only the explicit check
// detects a payload that claims more bytes than the stream holds.
func (w *frameWalker) skip(n int64) error {
	if err := w.ensure(n); err != nil {
		return err
	}

	if _, err := w.rs.Seek(n, io.SeekCurrent); err != nil {
		return fmt.Errorf("%w: skipping %d payload bytes at offset %d: %w", ErrCorruptZstdFrame, n, w.pos, err)
	}

	w.pos += n

	return nil
}

// readMagicAndHeader consumes the magic number and the complete frame header
// (Frame_Header_Descriptor, optional Window_Descriptor, Dictionary_ID and
// Frame_Content_Size fields), validating the magic and the descriptor's
// reserved/unused bits along the way.
func (w *frameWalker) readMagicAndHeader() (frameHeader, error) {
	magicBytes, err := w.read(magicLen)
	if err != nil {
		return frameHeader{}, err
	}

	magic := binary.LittleEndian.Uint32(magicBytes)
	frameStart := w.pos - magicLen

	if magic >= skippableMagicMin && magic <= skippableMagicMax {
		return frameHeader{}, fmt.Errorf("%w: skippable frame magic 0x%08X at offset %d", ErrCorruptZstdFrame, magic, frameStart)
	}

	if magic != zstdFrameMagic {
		return frameHeader{}, fmt.Errorf("%w: bad magic 0x%08X at offset %d", ErrCorruptZstdFrame, magic, frameStart)
	}

	fhdBytes, err := w.read(fhdLen)
	if err != nil {
		return frameHeader{}, err
	}

	return w.parseHeaderFields(fhdBytes[0])
}

// parseHeaderFields decodes the Frame_Header_Descriptor byte and consumes the
// variable-width header fields it announces. It rejects the reserved bit (3)
// and the unused bit (4): Codec.EncodeFrame never sets either, so a set bit
// signals a foreign or corrupt frame.
func (w *frameWalker) parseHeaderFields(fhd byte) (frameHeader, error) {
	fcsFlag := fhd >> 6
	singleSegment := fhd&(1<<5) != 0
	unusedBit := fhd&(1<<4) != 0
	reservedBit := fhd&(1<<3) != 0
	hasChecksum := fhd&(1<<2) != 0
	dictIDFlag := fhd & 0x03

	if reservedBit {
		return frameHeader{}, fmt.Errorf("%w: reserved FHD bit set (descriptor 0x%02X)", ErrCorruptZstdFrame, fhd)
	}

	if unusedBit {
		return frameHeader{}, fmt.Errorf("%w: unused FHD bit set (descriptor 0x%02X)", ErrCorruptZstdFrame, fhd)
	}

	// Window_Descriptor is present only when Single_Segment_flag is clear;
	// otherwise Frame_Content_Size doubles as the window size.
	if !singleSegment {
		if _, err := w.read(windowLen); err != nil {
			return frameHeader{}, err
		}
	}

	if didSize := dictIDFieldSize(dictIDFlag); didSize > 0 {
		if _, err := w.read(didSize); err != nil {
			return frameHeader{}, err
		}
	}

	if fcsSize := fcsFieldSize(fcsFlag, singleSegment); fcsSize > 0 {
		if _, err := w.read(fcsSize); err != nil {
			return frameHeader{}, err
		}
	}

	return frameHeader{hasChecksum: hasChecksum}, nil
}

// walkFrame advances the walker past one whole frame: magic, header, every data
// block, and the optional trailing content checksum.
func (w *frameWalker) walkFrame() error {
	hdr, err := w.readMagicAndHeader()
	if err != nil {
		return err
	}

	for {
		last, err := w.walkBlock()
		if err != nil {
			return err
		}

		if last {
			break
		}
	}

	if hdr.hasChecksum {
		if _, err := w.read(checksumLen); err != nil {
			return err
		}
	}

	return nil
}

// walkBlock consumes one block header and its physical payload, returning
// whether it was the last block of the frame. A Raw or Compressed block's
// physical payload is Block_Size bytes; an RLE block's physical payload is
// exactly one byte (the value repeated Block_Size times on decode), NOT
// Block_Size bytes.
func (w *frameWalker) walkBlock() (bool, error) {
	bhBytes, err := w.read(blockHeaderLen)
	if err != nil {
		return false, err
	}

	blockStart := w.pos - blockHeaderLen
	bh := uint32(bhBytes[0]) | uint32(bhBytes[1])<<8 | uint32(bhBytes[2])<<16
	last := bh&1 != 0
	blockType := (bh >> 1) & 0x03
	blockSize := int64(bh >> 3)

	switch blockType {
	case blockTypeRaw, blockTypeCompressed:
		if err := w.skip(blockSize); err != nil {
			return false, err
		}
	case blockTypeRLE:
		if err := w.skip(1); err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("%w: reserved block type at offset %d", ErrCorruptZstdFrame, blockStart)
	}

	return last, nil
}

// dictIDFieldSize maps the 2-bit Dictionary_ID_flag to its field width in bytes
// (RFC 8878 §3.1.1.1.1.3).
func dictIDFieldSize(flag byte) int {
	switch flag {
	case 1:
		return 1
	case 2:
		return 2
	case 3:
		return 4
	default:
		return 0
	}
}

// fcsFieldSize maps the 2-bit Frame_Content_Size_flag (and the Single_Segment_flag)
// to the Frame_Content_Size field width in bytes (RFC 8878 §3.1.1.1.1.4). The
// sole case where a clear flag still yields a field is FCS_flag == 0 with
// Single_Segment set, which mandates a 1-byte size.
func fcsFieldSize(flag byte, singleSegment bool) int {
	switch flag {
	case 1:
		return 2
	case 2:
		return 4
	case 3:
		return 8
	default:
		if singleSegment {
			return 1
		}

		return 0
	}
}

// addChecked returns a+b, rejecting a negative addend and any int64 overflow so
// a crafted block/field size can never wrap the running offset into a passing
// bound check.
func addChecked(a, b int64) (int64, error) {
	if b < 0 {
		return 0, fmt.Errorf("%w: negative length %d", ErrCorruptZstdFrame, b)
	}

	if a > math.MaxInt64-b {
		return 0, fmt.Errorf("%w: offset overflow (%d + %d)", ErrCorruptZstdFrame, a, b)
	}

	return a + b, nil
}
