package service

import (
	"archive/tar"
	"bytes"
	"io"
)

type deckhouseReleaseReader struct {
	versionReader *bytes.Buffer
}

func (rr *deckhouseReleaseReader) untarMetadata(rc io.Reader) error {
	tr := tar.NewReader(rc)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			// end of archive
			return nil
		}

		if err != nil {
			return err
		}

		switch hdr.Name {
		case "version.json":
			_, err = io.Copy(rr.versionReader, tr)
			if err != nil {
				return err
			}

		default:
			continue
		}
	}
}
