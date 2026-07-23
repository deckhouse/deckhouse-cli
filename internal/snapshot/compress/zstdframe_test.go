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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// synthBlock describes one data block of a hand-built frame. blockSize is the
// value written into the block header's 21-bit size field; physical is the
// bytes physically present after the block header (Block_Size bytes for
// Raw/Compressed, exactly one byte for RLE).
type synthBlock struct {
	blockType uint32
	last      bool
	blockSize int64
	physical  []byte
}

// synthFrame builds a zstd frame byte-for-byte so the test controls the exact
// physical length — the ground truth for boundary assertions is len() of these
// bytes, never a decoder's read-ahead position.
type synthFrame struct {
	fhd         byte
	headerExtra []byte // window descriptor, dictionary ID, FCS field bytes
	blocks      []synthBlock
	checksum    []byte // 4 bytes when the FHD checksum flag is set, else nil
}

func (f synthFrame) bytes() []byte {
	out := binary.LittleEndian.AppendUint32(nil, zstdFrameMagic)
	out = append(out, f.fhd)
	out = append(out, f.headerExtra...)

	for _, blk := range f.blocks {
		var last uint32
		if blk.last {
			last = 1
		}

		bh := uint32(blk.blockSize)<<3 | blk.blockType<<1 | last
		out = append(out, byte(bh), byte(bh>>8), byte(bh>>16))
		out = append(out, blk.physical...)
	}

	return append(out, f.checksum...)
}

// singleSegmentRaw builds a valid, independently-decodable single-segment frame
// carrying one Raw_Block of the given payload (len must be < 256 so the 1-byte
// FCS can hold the content size).
func singleSegmentRaw(t *testing.T, payload []byte) []byte {
	t.Helper()

	if len(payload) >= 256 {
		t.Fatalf("singleSegmentRaw payload too large for 1-byte FCS: %d", len(payload))
	}

	return synthFrame{
		fhd:         0x20, // Single_Segment set, FCS_flag 0 -> 1-byte FCS
		headerExtra: []byte{byte(len(payload))},
		blocks: []synthBlock{{
			blockType: blockTypeRaw,
			last:      true,
			blockSize: int64(len(payload)),
			physical:  payload,
		}},
	}.bytes()
}

func encodeRealFrames(t *testing.T, raws [][]byte) [][]byte {
	t.Helper()

	enc, err := NewEncoder(LevelDefault)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}

	frames := make([][]byte, 0, len(raws))

	for _, r := range raws {
		f, err := enc.EncodeFrame(r)
		if err != nil {
			t.Fatalf("EncodeFrame: %v", err)
		}

		frames = append(frames, f)
	}

	return frames
}

// cumulativeOffsets returns the ground-truth frame boundaries: offsets[i] is the
// sum of the physical lengths of frames 0..i-1. These are byte lengths of the
// encoded slices, deliberately NOT anything a decoder reports.
func cumulativeOffsets(frames [][]byte) []int64 {
	offsets := make([]int64, len(frames)+1)
	for i, f := range frames {
		offsets[i+1] = offsets[i] + int64(len(f))
	}

	return offsets
}

func pseudoRandom(n int, seed uint64) []byte {
	b := make([]byte, n)
	x := seed

	for i := range b {
		x = x*6364136223846793005 + 1442695040888963407
		b[i] = byte(x >> 56)
	}

	return b
}

func TestSkipZstdFrames_RealFrameBoundaries(t *testing.T) {
	t.Parallel()

	raws := [][]byte{
		[]byte("the quick brown fox"),
		bytes.Repeat([]byte("A"), 300_000), // highly compressible -> compressed multi-block
		pseudoRandom(200_000, 0x1234),      // incompressible -> raw multi-block
		[]byte("tiny"),
		bytes.Repeat([]byte("xy"), 100),
	}

	frames := encodeRealFrames(t, raws)
	concat := bytes.Join(frames, nil)
	offsets := cumulativeOffsets(frames)

	for i := range frames {
		got, err := SkipZstdFrames(bytes.NewReader(concat), i)
		if err != nil {
			t.Fatalf("SkipZstdFrames(_, %d): unexpected error: %v", i, err)
		}

		if got != offsets[i] {
			t.Errorf("frame %d boundary = %d, want %d", i, got, offsets[i])
		}
	}

	// Asking for frame == frameCount has no frame to validate and must fail.
	if _, err := SkipZstdFrames(bytes.NewReader(concat), len(frames)); !errors.Is(err, ErrCorruptZstdFrame) {
		t.Errorf("out-of-range index: got err %v, want ErrCorruptZstdFrame", err)
	}
}

// TestSkipZstdFrames_DecodedBytesMatch independently confirms the concatenation
// the boundary test walks is a genuine multi-frame stream, using the decoder
// ONLY to compare decoded bytes against the original inputs.
func TestSkipZstdFrames_DecodedBytesMatch(t *testing.T) {
	t.Parallel()

	raws := [][]byte{
		[]byte("frame-zero"),
		bytes.Repeat([]byte("Z"), 5000),
		pseudoRandom(4096, 0xBEEF),
	}

	frames := encodeRealFrames(t, raws)
	concat := bytes.Join(frames, nil)

	dec, err := zstd.NewReader(bytes.NewReader(concat))
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	defer dec.Close()

	got, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("decode concat: %v", err)
	}

	if want := bytes.Join(raws, nil); !bytes.Equal(got, want) {
		t.Errorf("decoded %d bytes, want %d", len(got), len(want))
	}
}

func TestSkipZstdFrames_RestoresSeekerPosition(t *testing.T) {
	t.Parallel()

	frames := encodeRealFrames(t, [][]byte{[]byte("aaaa"), []byte("bbbb"), []byte("cccc")})
	concat := bytes.Join(frames, nil)

	for n := range len(frames) {
		r := bytes.NewReader(concat)

		if _, err := SkipZstdFrames(r, n); err != nil {
			t.Fatalf("SkipZstdFrames(_, %d): %v", n, err)
		}

		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatalf("Seek current: %v", err)
		}

		if pos != 0 {
			t.Errorf("n=%d: reader left at offset %d, want 0 (non-destructive)", n, pos)
		}
	}
}

func TestSkipZstdFrames_NegativeCount(t *testing.T) {
	t.Parallel()

	frames := encodeRealFrames(t, [][]byte{[]byte("x")})

	// A negative count is an argument error, deliberately NOT ErrCorruptZstdFrame.
	_, err := SkipZstdFrames(bytes.NewReader(bytes.Join(frames, nil)), -1)
	if err == nil {
		t.Fatal("negative count: expected an error, got nil")
	}

	if errors.Is(err, ErrCorruptZstdFrame) {
		t.Errorf("negative count wrapped ErrCorruptZstdFrame, want a plain argument error: %v", err)
	}
}

// TestSkipZstdFrames_RLEPhysicalOneByte is the discriminating test for the RLE
// rule: the RLE block declares Block_Size 200 but occupies exactly one physical
// byte. An implementation that skipped Block_Size (200) bytes would overshoot
// the frame and land inside — or past — the following frame, so the boundary
// would not equal len(rle) and target validation would fail.
func TestSkipZstdFrames_RLEPhysicalOneByte(t *testing.T) {
	t.Parallel()

	rle := synthFrame{
		fhd:         0x20, // Single_Segment, 1-byte FCS
		headerExtra: []byte{200},
		blocks: []synthBlock{{
			blockType: blockTypeRLE,
			last:      true,
			blockSize: 200,
			physical:  []byte{0xAA},
		}},
	}.bytes()

	// Independent confirmation the RLE frame is well-formed and physically one
	// byte of payload: decode it and expect 200 repeats of 0xAA.
	dec, err := zstd.NewReader(bytes.NewReader(rle))
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}

	decoded, err := io.ReadAll(dec)
	dec.Close()

	if err != nil {
		t.Fatalf("decode RLE frame: %v", err)
	}

	if want := bytes.Repeat([]byte{0xAA}, 200); !bytes.Equal(decoded, want) {
		t.Fatalf("RLE decoded %d bytes, want 200 repeats", len(decoded))
	}

	real0 := encodeRealFrames(t, [][]byte{[]byte("after-rle")})[0]
	concat := append(append([]byte{}, rle...), real0...)

	got, err := SkipZstdFrames(bytes.NewReader(concat), 1)
	if err != nil {
		t.Fatalf("SkipZstdFrames(_, 1): %v", err)
	}

	if want := int64(len(rle)); got != want {
		t.Errorf("boundary after RLE frame = %d, want %d (RLE payload must be 1 physical byte)", got, want)
	}
}

// TestSkipZstdFrames_HeaderWidths drives every Frame_Header_Descriptor field
// width. Each case places a synthetic first frame with a distinct header shape
// ahead of a real second frame; the boundary MUST equal the synthetic frame's
// literal byte length, which independently proves the header width (and the
// single raw block) were consumed exactly.
func TestSkipZstdFrames_HeaderWidths(t *testing.T) {
	t.Parallel()

	real1 := encodeRealFrames(t, [][]byte{[]byte("second-frame-payload")})[0]
	payload := []byte{1, 2, 3, 4, 5}
	rawBlock := synthBlock{blockType: blockTypeRaw, last: true, blockSize: int64(len(payload)), physical: payload}

	cases := []struct {
		name        string
		fhd         byte
		headerExtra []byte
	}{
		{"single_segment_fcs1", 0x20, []byte{0x05}},
		{"window_fcs0", 0x00, []byte{0x00}},
		{"window_fcs2", 0x40, []byte{0x00, 0xAB, 0xCD}},
		{"window_fcs4", 0x80, []byte{0x00, 1, 2, 3, 4}},
		{"window_fcs8", 0xC0, []byte{0x00, 1, 2, 3, 4, 5, 6, 7, 8}},
		{"single_segment_dictid1", 0x21, []byte{0x07, 0x05}},       // DID(1)+FCS(1)
		{"single_segment_dictid2", 0x22, []byte{0x07, 0x08, 0x05}}, // DID(2)+FCS(1)
		{"single_segment_dictid4", 0x23, []byte{1, 2, 3, 4, 0x05}}, // DID(4)+FCS(1)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			first := synthFrame{fhd: tc.fhd, headerExtra: tc.headerExtra, blocks: []synthBlock{rawBlock}}.bytes()
			concat := append(append([]byte{}, first...), real1...)

			got, err := SkipZstdFrames(bytes.NewReader(concat), 1)
			if err != nil {
				t.Fatalf("SkipZstdFrames(_, 1): %v", err)
			}

			if want := int64(len(first)); got != want {
				t.Errorf("boundary = %d, want %d", got, want)
			}
		})
	}
}

// TestSkipZstdFrames_MultiBlock exercises the block loop and last-block
// detection over a frame with two Raw_Blocks, and confirms the frame decodes.
func TestSkipZstdFrames_MultiBlock(t *testing.T) {
	t.Parallel()

	b1 := []byte("hello ")
	b2 := []byte("world!")

	multi := synthFrame{
		fhd:         0x00, // window descriptor, no FCS
		headerExtra: []byte{0x00},
		blocks: []synthBlock{
			{blockType: blockTypeRaw, last: false, blockSize: int64(len(b1)), physical: b1},
			{blockType: blockTypeRaw, last: true, blockSize: int64(len(b2)), physical: b2},
		},
	}.bytes()

	dec, err := zstd.NewReader(bytes.NewReader(multi))
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}

	decoded, err := io.ReadAll(dec)
	dec.Close()

	if err != nil {
		t.Fatalf("decode multi-block frame: %v", err)
	}

	if want := append(append([]byte{}, b1...), b2...); !bytes.Equal(decoded, want) {
		t.Fatalf("multi-block decoded %q, want %q", decoded, want)
	}

	real1 := encodeRealFrames(t, [][]byte{[]byte("next")})[0]
	concat := append(append([]byte{}, multi...), real1...)

	got, err := SkipZstdFrames(bytes.NewReader(concat), 1)
	if err != nil {
		t.Fatalf("SkipZstdFrames(_, 1): %v", err)
	}

	if want := int64(len(multi)); got != want {
		t.Errorf("boundary after multi-block frame = %d, want %d", got, want)
	}
}

func TestSkipZstdFrames_MalformedFrames(t *testing.T) {
	t.Parallel()

	rawBlk := synthBlock{blockType: blockTypeRaw, last: true, blockSize: 3, physical: []byte{1, 2, 3}}

	cases := []struct {
		name string
		data []byte
		n    int // reserved block type is caught only by walking a frame (n=1)
	}{
		{
			name: "reserved_fhd_bit",
			data: synthFrame{fhd: 0x20 | (1 << 3), headerExtra: []byte{0x03}, blocks: []synthBlock{rawBlk}}.bytes(),
			n:    0,
		},
		{
			name: "unused_fhd_bit",
			data: synthFrame{fhd: 0x20 | (1 << 4), headerExtra: []byte{0x03}, blocks: []synthBlock{rawBlk}}.bytes(),
			n:    0,
		},
		{
			name: "reserved_block_type",
			data: synthFrame{fhd: 0x20, headerExtra: []byte{0x03}, blocks: []synthBlock{
				{blockType: 3, last: true, blockSize: 3, physical: []byte{1, 2, 3}},
			}}.bytes(),
			n: 1,
		},
		{
			name: "skippable_magic",
			data: binary.LittleEndian.AppendUint32(nil, skippableMagicMin),
			n:    0,
		},
		{
			name: "unknown_magic",
			data: []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00},
			n:    0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := SkipZstdFrames(bytes.NewReader(tc.data), tc.n); !errors.Is(err, ErrCorruptZstdFrame) {
				t.Errorf("got err %v, want ErrCorruptZstdFrame", err)
			}
		})
	}
}

func TestSkipZstdFrames_Truncation(t *testing.T) {
	t.Parallel()

	frame0 := singleSegmentRaw(t, []byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 100})
	frame1 := singleSegmentRaw(t, []byte("second"))
	twoFrames := append(append([]byte{}, frame0...), frame1...)

	realWithChecksum := encodeRealFrames(t, [][]byte{bytes.Repeat([]byte("Q"), 2048)})[0]

	cases := []struct {
		name string
		data []byte
		n    int
	}{
		// n=0 validates only the target frame's magic+header, so a header-region
		// truncation is caught at n=0.
		{"empty_stream_frame0", nil, 0},
		{"magic_cut", frame0[:2], 0},
		{"fhd_cut", frame0[:magicLen], 0},
		{"header_field_cut", frame0[:magicLen+fhdLen], 0}, // FCS byte missing
		// A block-header/payload/checksum truncation is caught only while WALKING
		// a frame, so these force a walk of the truncated frame 0 via n=1.
		{"block_header_cut", frame0[:magicLen+fhdLen+1+1], 1},
		{"payload_cut", frame0[:len(frame0)-3], 1},
		{"checksum_cut", realWithChecksum[:len(realWithChecksum)-2], 1},
		{"second_frame_missing", twoFrames[:len(frame0)+2], 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := SkipZstdFrames(bytes.NewReader(tc.data), tc.n); !errors.Is(err, ErrCorruptZstdFrame) {
				t.Errorf("got err %v, want ErrCorruptZstdFrame", err)
			}
		})
	}
}

// TestSkipZstdFrames_TruncationIndependentOfSeek proves the bounds check, not a
// failing Seek, is what detects a payload that runs past EOF: a bare Seek to the
// same beyond-EOF offset on the identical reader succeeds without error, yet the
// walker still reports corruption.
func TestSkipZstdFrames_TruncationIndependentOfSeek(t *testing.T) {
	t.Parallel()

	frame0 := singleSegmentRaw(t, bytes.Repeat([]byte{0x7E}, 100))
	truncated := frame0[:len(frame0)-40] // 40 payload bytes missing

	// A raw Seek past EOF succeeds for a bytes.Reader — Seek success proves nothing.
	probe := bytes.NewReader(truncated)
	if _, err := probe.Seek(int64(len(frame0)), io.SeekStart); err != nil {
		t.Fatalf("precondition: Seek past EOF should succeed for bytes.Reader, got %v", err)
	}

	if _, err := SkipZstdFrames(bytes.NewReader(truncated), 1); !errors.Is(err, ErrCorruptZstdFrame) {
		t.Errorf("got err %v, want ErrCorruptZstdFrame from the bounds check", err)
	}
}

// countingSeeker records the exact bytes handed to Read and the number of Seek
// calls, so a test can prove the walker reads only headers (never payloads) and
// never wraps the reader in a buffering layer that would desync the offset.
type countingSeeker struct {
	inner     io.ReadSeeker
	readBytes int64
	seekCalls int
}

func (c *countingSeeker) Read(p []byte) (int, error) {
	n, err := c.inner.Read(p)
	c.readBytes += int64(n)

	return n, err
}

func (c *countingSeeker) Seek(offset int64, whence int) (int64, error) {
	c.seekCalls++

	return c.inner.Seek(offset, whence)
}

func TestSkipZstdFrames_BoundedHeaderReadsNoPayloadReads(t *testing.T) {
	t.Parallel()

	const bigPayload = 4096

	frame0 := singleSegmentRaw(t, []byte{0}) // placeholder, replaced below
	// Build frame0 with a large raw payload directly (payload len > 255 needs a
	// wider FCS, so use the window/FCS0 shape rather than singleSegmentRaw).
	frame0 = synthFrame{
		fhd:         0x00,
		headerExtra: []byte{0x00},
		blocks: []synthBlock{{
			blockType: blockTypeRaw,
			last:      true,
			blockSize: bigPayload,
			physical:  bytes.Repeat([]byte{0x55}, bigPayload),
		}},
	}.bytes()

	frame1 := singleSegmentRaw(t, []byte("tail"))
	concat := append(append([]byte{}, frame0...), frame1...)

	cs := &countingSeeker{inner: bytes.NewReader(concat)}

	got, err := SkipZstdFrames(cs, 1)
	if err != nil {
		t.Fatalf("SkipZstdFrames(_, 1): %v", err)
	}

	if want := int64(len(frame0)); got != want {
		t.Fatalf("boundary = %d, want %d", got, want)
	}

	// Frame0 header: magic(4)+fhd(1)+window(1)+blockheader(3) = 9 bytes read,
	// its 4096-byte payload SKIPPED. Target validation reads frame1's
	// magic(4)+fhd(1)+fcs(1) = 6 bytes, and no block. Total = 15 header bytes.
	const wantRead = 9 + 6
	if cs.readBytes != wantRead {
		t.Errorf("read %d bytes, want %d (payload must be seeked, never read)", cs.readBytes, wantRead)
	}

	if cs.readBytes >= bigPayload {
		t.Errorf("read %d bytes >= payload %d: payload was read, not seeked", cs.readBytes, bigPayload)
	}

	if cs.seekCalls == 0 {
		t.Error("expected at least one Seek (the payload skip), got none")
	}
}

func TestAddChecked(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		a, b    int64
		want    int64
		wantErr bool
	}{
		{"simple", 10, 5, 15, false},
		{"zero_addend", 100, 0, 100, false},
		{"max_boundary", math.MaxInt64 - 5, 5, math.MaxInt64, false},
		{"overflow", math.MaxInt64, 1, 0, true},
		{"overflow_large", math.MaxInt64 - 3, 10, 0, true},
		{"negative_addend", 10, -1, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := addChecked(tc.a, tc.b)
			if tc.wantErr {
				if !errors.Is(err, ErrCorruptZstdFrame) {
					t.Errorf("got err %v, want ErrCorruptZstdFrame", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tc.want {
				t.Errorf("addChecked(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestFieldSizeTables(t *testing.T) {
	t.Parallel()

	t.Run("dictIDFieldSize", func(t *testing.T) {
		t.Parallel()

		for flag, want := range map[byte]int{0: 0, 1: 1, 2: 2, 3: 4} {
			if got := dictIDFieldSize(flag); got != want {
				t.Errorf("dictIDFieldSize(%d) = %d, want %d", flag, got, want)
			}
		}
	})

	t.Run("fcsFieldSize", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			flag          byte
			singleSegment bool
			want          int
		}{
			{0, false, 0},
			{0, true, 1},
			{1, false, 2},
			{1, true, 2},
			{2, false, 4},
			{3, false, 8},
		}

		for _, tc := range cases {
			if got := fcsFieldSize(tc.flag, tc.singleSegment); got != tc.want {
				t.Errorf("fcsFieldSize(%d, %t) = %d, want %d", tc.flag, tc.singleSegment, got, tc.want)
			}
		}
	})
}
