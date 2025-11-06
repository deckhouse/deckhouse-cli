package platform

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
	imagesDigestsFile = "deckhouse/candi/images_digests.json"
	imagesTagsFile    = "deckhouse/candi/images_tags.json"
)

type deckhouseInstallerReader struct {
	imageDigestsReader *bytes.Buffer
	imageTagsReader    *bytes.Buffer
}

func (rr *deckhouseInstallerReader) untarMetadata(rc io.Reader) error {
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

		if strings.Contains(hdr.Name, "deckhouse/candi") {
			fmt.Println(hdr.Name)
		}

		switch hdr.Name {
		case "version.json":
			_, err = io.Copy(rr.imageDigestsReader, tr)
			if err != nil {
				return err
			}

		default:
			continue
		}
	}
}
