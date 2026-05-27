// Package iconutil discovers the package icon at the package root and
// exposes its on-disk and decoded properties as a single Icon value.
//
// The convention is `icon.<ext>` at the package root. Find walks any matching
// file (not only registered formats) so callers can validate the extension as
// a separate concern; supported formats live in the unexported formats
// registry — adding one is a single entry plus the matching import.
package iconutil

import (
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/image/webp"
)

// Icon is a discovered package icon with its on-disk and decoded properties.
type Icon struct {
	// Path is the absolute path to the icon file on disk.
	Path string
	// Ext is the file extension including the leading dot (e.g. ".png").
	// May be an unsupported extension — callers should validate it (see ExtRule).
	Ext string
	// Size is the icon file size in bytes.
	Size int64
	// Shape carries the rendered width and height. Zero when the format is
	// unsupported, decoding failed, or the format is vector with no intrinsic
	// rasterized size.
	Shape image.Config
}

// format is an internal registry entry: an extension and its decoder.
type format struct {
	ext          string
	decodeConfig func(io.Reader) (image.Config, error)
}

// formats lists supported icon formats in discovery-priority order. When
// multiple icon files exist, the first match wins. PNG and WebP (transparency-
// capable) sit ahead of JPEG; SVG is last because rasterized formats are
// preferred where available.
var formats = []format{
	{ext: ".png", decodeConfig: png.DecodeConfig},
	{ext: ".webp", decodeConfig: webp.DecodeConfig},
	{ext: ".jpg", decodeConfig: jpeg.DecodeConfig},
	{ext: ".jpeg", decodeConfig: jpeg.DecodeConfig},
	{ext: ".svg", decodeConfig: svgDecodeConfig},
}

// ErrNoIcon is returned by Find when no icon.* file is present at packageDir.
var ErrNoIcon = errors.New("icon not found")

// Find discovers any icon.* file at packageDir, reads its size, and tries to
// decode its dimensions when the extension is supported.
//
// Returned states:
//   - (Icon{}, ErrNoIcon)              no icon.* file exists
//   - (Icon{Path,Ext,Size}, decodeErr) content cannot be decoded
//   - (Icon{Path,Ext,Size}, nil)       extension unsupported (Shape zero)
//   - (Icon{Path,Ext,Size,Shape}, nil) full success
//   - (partial Icon, os-level error)   filesystem ops failed
func Find(packageDir string) (Icon, error) {
	matches, err := filepath.Glob(filepath.Join(packageDir, "icon.*"))
	if err != nil {
		return Icon{}, err
	}

	if len(matches) == 0 {
		return Icon{}, ErrNoIcon
	}

	path := pickPriority(matches)
	icon := Icon{Path: path, Ext: filepath.Ext(path)}

	file, err := os.Open(path)
	if err != nil {
		return icon, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return icon, err
	}

	icon.Size = info.Size()

	decoder := decoderFor(icon.Ext)
	if decoder == nil {
		// Unsupported extension — caller validates via AllowedExts.
		return icon, nil
	}

	cfg, decodeErr := decoder(file)
	if decodeErr != nil {
		return icon, fmt.Errorf("icon content does not match extension: %v", decodeErr)
	}

	icon.Shape = cfg

	return icon, nil
}

// pickPriority chooses one path from glob matches: prefer registered formats
// in priority order; fall back to the first match (which may have an
// unsupported extension).
func pickPriority(matches []string) string {
	byExt := make(map[string]string, len(matches))
	for _, m := range matches {
		byExt[filepath.Ext(m)] = m
	}

	for _, f := range formats {
		if path, ok := byExt[f.ext]; ok {
			return path
		}
	}

	return matches[0]
}

// decoderFor returns the registered decoder for ext, or nil when ext is unsupported.
func decoderFor(ext string) func(io.Reader) (image.Config, error) {
	for _, f := range formats {
		if f.ext == ext {
			return f.decodeConfig
		}
	}

	return nil
}

// Expected returns the conventional icon path(s) for diagnostics — "icon.png"
// when only one format is registered, "icon.{png,webp,...}" otherwise.
func Expected() string {
	if len(formats) == 1 {
		return "icon" + formats[0].ext
	}

	exts := make([]string, len(formats))
	for i, f := range formats {
		exts[i] = strings.TrimPrefix(f.ext, ".")
	}

	return "icon.{" + strings.Join(exts, ",") + "}"
}
