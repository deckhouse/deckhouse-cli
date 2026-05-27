package imagefs

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	crv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// ExtractToTemp downloads srcRef, extracts its filesystem to a temp directory, and returns that path.
// The caller is responsible for removing the returned directory.
func ExtractToTemp(ctx context.Context, srcRef string) (string, error) {
	tmp, err := os.MkdirTemp("", "imagefs-*")
	if err != nil {
		return "", fmt.Errorf("create temp directory: %w", err)
	}

	if err = extractReference(ctx, srcRef, tmp); err != nil {
		if removeErr := os.RemoveAll(tmp); removeErr != nil {
			return "", fmt.Errorf("%w; remove temp directory: %v", err, removeErr)
		}

		return "", err
	}

	return tmp, nil
}

// extractReference resolves srcRef, downloads the image, and extracts it into output.
func extractReference(ctx context.Context, srcRef, output string) error {
	ref, err := name.ParseReference(srcRef, name.Insecure)
	if err != nil {
		return fmt.Errorf("parse reference: %w", err)
	}

	img, err := remote.Image(ref, remote.WithAuthFromKeychain(authn.DefaultKeychain), remote.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("get image: %w", err)
	}

	return extract(ctx, img, output)
}

// extract unpacks img into output while rejecting tar entries that escape output.
func extract(ctx context.Context, img crv1.Image, output string) error {
	rc := mutate.Extract(img)
	defer rc.Close()

	if err := os.MkdirAll(output, 0o700); err != nil {
		return fmt.Errorf("create output path: %w", err)
	}

	output, err := filepath.Abs(output)
	if err != nil {
		return fmt.Errorf("resolve output path: %w", err)
	}

	tr := tar.NewReader(rc)

	for {
		if err = ctx.Err(); err != nil {
			return err
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}

		target, err := safeJoin(output, hdr.Name)
		if err != nil {
			return err
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err = os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return fmt.Errorf("mkdir %q: %w", hdr.Name, err)
			}
		case tar.TypeReg:
			if err = writeRegularFile(target, tr, os.FileMode(hdr.Mode)); err != nil {
				return fmt.Errorf("write file %q: %w", hdr.Name, err)
			}
		case tar.TypeSymlink:
			if filepath.IsAbs(hdr.Linkname) || !staysWithin(output, filepath.Dir(target), hdr.Linkname) {
				return fmt.Errorf("symlink %q escapes output directory", hdr.Name)
			}

			if err = os.Symlink(hdr.Linkname, target); err != nil {
				return fmt.Errorf("create symlink %q: %w", hdr.Name, err)
			}
		case tar.TypeLink:
			linkTarget, err := safeJoin(output, hdr.Linkname)
			if err != nil {
				return err
			}

			if err = os.Link(linkTarget, target); err != nil {
				return fmt.Errorf("create hardlink %q: %w", hdr.Name, err)
			}
		}
	}

	return nil
}

// writeRegularFile writes one regular tar entry and limits restored permissions to owner bits.
func writeRegularFile(target string, src io.Reader, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}

	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode&0o700)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	if _, err = io.Copy(out, src); err != nil {
		closeErr := out.Close()
		if closeErr != nil {
			return fmt.Errorf("copy file: %w; close file: %v", err, closeErr)
		}

		return fmt.Errorf("copy file: %w", err)
	}

	if err = out.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	return nil
}

// safeJoin joins name under root and rejects absolute paths or parent-directory escapes.
func safeJoin(root, name string) (string, error) {
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("path %q escapes output directory", name)
	}

	target := filepath.Join(root, name)
	if !staysWithin(root, root, name) {
		return "", fmt.Errorf("path %q escapes output directory", name)
	}

	return target, nil
}

// staysWithin reports whether name resolves under root when interpreted relative to base.
func staysWithin(root, base, name string) bool {
	target := filepath.Clean(filepath.Join(base, name))
	rel, err := filepath.Rel(root, target)

	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
