package iconutil

import (
	"encoding/xml"
	"errors"
	"fmt"
	"image"
	"io"
	"regexp"
	"strconv"
)

// svgPxLength matches a non-negative SVG length expressed in pixels — either
// unitless (treated as pixels per the CSS spec) or with an explicit "px" suffix.
// Anything else (em, percent, rem, vw, …) intentionally does not match: such
// values have no fixed pixel size, so the rasterized 300×300 limit cannot be
// enforced and we treat the dimension as unknown.
var svgPxLength = regexp.MustCompile(`^\s*(\d+(?:\.\d+)?)\s*(?:px)?\s*$`)

// svgDecodeConfig validates that r is an SVG document and extracts its width
// and height when they are expressed in pixels.
//
// Width/Height are zero when the value is missing, in a non-pixel unit, or
// unparseable. IconRule treats zero as "no rasterized size" and skips the
// max-dimension check — SVG is vector and has no inherent rendering size in
// those cases.
func svgDecodeConfig(r io.Reader) (image.Config, error) {
	dec := xml.NewDecoder(r)

	// Walk past the XML prolog, comments, and processing instructions until
	// the first start element — that must be <svg>. Anything else is rejected.
	for {
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return image.Config{}, errors.New("not an SVG: no root element")
			}

			return image.Config{}, fmt.Errorf("not an SVG: %w", err)
		}

		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		if start.Name.Local != "svg" {
			return image.Config{}, fmt.Errorf("not an SVG: root element is <%s>", start.Name.Local)
		}

		var width, height int

		for _, attr := range start.Attr {
			switch attr.Name.Local {
			case "width":
				width = parseSVGPxLength(attr.Value)
			case "height":
				height = parseSVGPxLength(attr.Value)
			}
		}

		return image.Config{Width: width, Height: height}, nil
	}
}

// parseSVGPxLength parses an SVG length value. Returns the pixel count for
// pixel-expressed lengths and 0 for anything else (so the limit check skips it).
func parseSVGPxLength(s string) int {
	match := svgPxLength.FindStringSubmatch(s)
	if match == nil {
		return 0
	}

	f, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0
	}

	return int(f)
}
