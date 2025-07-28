package gostsum

import (
	"bufio"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/spf13/cobra"
	streebog256 "go.cypherpunks.ru/gogost/v5/gost34112012256"
	streebog512 "go.cypherpunks.ru/gogost/v5/gost34112012512"
)

func Gostsum(cmd *cobra.Command, args []string) error {
	length, err := cmd.Flags().GetInt("hash-length")
	if err != nil {
		panic(err)
	}

	var hasher hash.Hash
	switch length {
	case 256:
		hasher = streebog256.New()
	case 512:
		hasher = streebog512.New()
	default:
		return fmt.Errorf("invalid hash length: %d", length)
	}

	// No input files, read from stdin
	if len(args) == 0 {
		checksum, err := digest(os.Stdin, hasher)
		if err != nil {
			return err
		}
		if _, err = fmt.Println(checksum); err != nil {
			return err
		}
		return nil
	}

	// Have a set of file paths to hash provided as args
	var reader io.Reader
	var checksum string
	for _, filepath := range args {
		reader, err = os.Open(filepath)
		if err != nil {
			return fmt.Errorf("%s: %w", filepath, err)
		}
		checksum, err = digest(bufio.NewReader(reader), hasher)
		if err != nil {
			return err
		}
		if _, err = fmt.Println(filepath+":", checksum); err != nil {
			return err
		}
	}

	return nil
}

func digest(blobStream io.Reader, hasher hash.Hash) (string, error) {
	if _, err := io.Copy(hasher, blobStream); err != nil {
		return "", fmt.Errorf("digest blob: %w", err)
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}
