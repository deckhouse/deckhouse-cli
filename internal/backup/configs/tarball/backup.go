package tarball

import (
	"archive/tar"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

type Backup struct {
	mu     sync.Mutex
	writer *tar.Writer
}

type BackupResourcesFilter interface {
	Matches(object runtime.Object) bool
}

func NewBackup(sink io.Writer) *Backup {
	return &Backup{
		writer: tar.NewWriter(sink),
	}
}

func (b *Backup) PutObject(object runtime.Object) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	metadataAccessor, err := meta.Accessor(object)
	if err != nil {
		return fmt.Errorf("%w: %s does not contain metadata to filter with", err, object.GetObjectKind().GroupVersionKind().String())
	}

	metadataAccessor.SetManagedFields(nil)

	kind := object.GetObjectKind().GroupVersionKind().Kind
	name, namespace := metadataAccessor.GetName(), metadataAccessor.GetNamespace()
	if namespace == "" {
		namespace = "Cluster-Scoped"
	}

	rawObject, err := yaml.Marshal(object)
	if err != nil {
		return fmt.Errorf("marshal %s %s/%s: %w", kind, namespace, name, err)
	}

	err = b.writer.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     path.Join(namespace, kind, name+".yml"),
		Size:     int64(len(rawObject)),
		Mode:     0600,
		ModTime:  time.Now(),
	})
	if err != nil {
		return fmt.Errorf("write tar header for %s %s/%s: %w", kind, namespace, name, err)
	}

	if _, err = b.writer.Write(rawObject); err != nil {
		return fmt.Errorf("write tar content for %s %s/%s: %w", kind, namespace, name, err)
	}

	return nil
}

func (b *Backup) Close() error {
	return b.writer.Close()
}
