package progress

import (
	"bytes"
	"fmt"
	"testing"
)

func TestProgressCallback(t *testing.T) {
	const input = "Hello World!"
	ctx := t.Context()

	// Check the progress bar, or invisible copy-through, works at all sylog
	// levels

	visibilities := []bool{
		true,
		false,
	}

	for _, v := range visibilities {
		t.Run(fmt.Sprintf("visible: %v", v), func(t *testing.T) {
			Visible = v

			cb := BarCallback(ctx)
			src := bytes.NewBufferString(input)
			dst := bytes.Buffer{}

			err := cb(int64(len(input)), src, &dst)
			if err != nil {
				t.Errorf("Unexpected error from ProgressCallBack: %v", err)
			}

			output := dst.String()
			if output != input {
				t.Errorf("Output from callback '%s' != input '%s'", output, input)
			}
		})
	}
}
