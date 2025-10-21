package registry

import (
	"io"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

// LayerStream represents a single layer stream for extraction
type LayerStream struct {
	index  int
	total  int
	reader io.ReadCloser
}

// Ensure LayerStream implements pkg.LayerStreamInterface
var _ pkg.LayerStream = (*LayerStream)(nil)

// GetIndex returns the current layer index (1-based)
func (ls *LayerStream) GetIndex() int {
	return ls.index
}

// GetTotal returns the total number of layers
func (ls *LayerStream) GetTotal() int {
	return ls.total
}

// GetReader returns the reader for the layer content
func (ls *LayerStream) GetReader() io.ReadCloser {
	return ls.reader
}

// NewLayerStream creates a new LayerStream
func NewLayerStream(index, total int, reader io.ReadCloser) *LayerStream {
	return &LayerStream{
		index:  index,
		total:  total,
		reader: reader,
	}
}
