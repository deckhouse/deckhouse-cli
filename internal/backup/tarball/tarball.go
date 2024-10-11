package tarball

import (
	"archive/tar"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

type Backup struct {
	mu     sync.Mutex
	writer *tar.Writer
}

func NewBackup(sink io.Writer) *Backup {
	return &Backup{
		writer: tar.NewWriter(sink),
	}
}

func (b *Backup) PutResources(resources []unstructured.Unstructured) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	for _, resource := range resources {
		rawSecret, err := yaml.Marshal(resource)
		if err != nil {
			return fmt.Errorf(
				"marshalling %s %s/%s: %w",
				resource.GetKind(),
				resource.GetNamespace(),
				resource.GetName(),
				err,
			)
		}

		err = b.writer.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     path.Join(resource.GetNamespace(), resource.GetKind(), resource.GetName()+".yml"),
			Size:     int64(len(rawSecret)),
			Mode:     0600,
			ModTime:  now,
		})
		if err != nil {
			return fmt.Errorf(
				"writing tar header for %s %s/%s: %w",
				resource.GetKind(),
				resource.GetNamespace(),
				resource.GetName(),
				err,
			)
		}

		if _, err = b.writer.Write(rawSecret); err != nil {
			return fmt.Errorf(
				"writing tar content for %s %s/%s: %w",
				resource.GetKind(),
				resource.GetNamespace(),
				resource.GetName(),
				err,
			)
		}
	}

	return nil
}

func (b *Backup) Close() error {
	return b.writer.Close()
}
