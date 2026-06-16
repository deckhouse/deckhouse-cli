/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package fswalk implements the data-exporter filesystem download protocol: a recursive walk over the
// pod's /api/v1/files directory-listing JSON, downloading each file in parallel. It is shared by
// `d8 data export download` and `d8 snapshot export download` (Filesystem volume mode), so neither has
// to re-implement the listing format or the bounded-parallelism descent.
package fswalk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"sync"
)

// DefaultConcurrency bounds the number of in-flight file downloads during a recursive walk.
const DefaultConcurrency = 10

// Doer is the minimal HTTP client surface the walker needs (satisfied by *libsaferequest.SafeClient).
// Depending on this interface instead of the concrete client keeps fswalk free of the kube client
// dependency and lets the recursive descent be unit-tested against an httptest server.
type Doer interface {
	HTTPDo(req *http.Request) (*http.Response, error)
}

// Directory-listing entry types returned by the data-exporter under "items".
const (
	itemTypeDir  = "dir"
	itemTypeFile = "file"
	itemTypeLink = "link"
)

// dirItem is one entry of the data-exporter directory listing.
type dirItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// forRespItems streams the data-exporter directory-listing JSON ({"items": [{name,type}, ...]}) and
// invokes workFunc for each entry, without buffering the whole listing.
func forRespItems(jsonStream io.ReadCloser, workFunc func(*dirItem) error) error {
	dec := json.NewDecoder(jsonStream)

	// find items list
	for {
		t, err := dec.Token()
		if err != nil {
			return err
		}

		if t == "items" {
			t, err := dec.Token()
			if err != nil {
				return err
			}

			if t != json.Delim('[') {
				return fmt.Errorf("JSON items is not list")
			}

			break
		}

		dec.More()
	}

	// read items
	for dec.More() {
		var i dirItem

		err := dec.Decode(&i)
		if err != nil {
			break
		}

		err = workFunc(&i)
		if err != nil {
			return err
		}
	}

	// check items list closed
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if t != json.Delim(']') {
		return fmt.Errorf("items loading is not completed")
	}

	return nil
}

// RecursiveDownload downloads the data-exporter resource at url+srcPath into dstPath. A srcPath ending
// in "/" is treated as a directory: its listing is fetched and each entry downloaded in parallel
// (bounded by sem); otherwise the single file is streamed to dstPath (or stdout when dstPath is empty).
func RecursiveDownload(ctx context.Context, sClient Doer, log *slog.Logger, sem chan struct{}, url, srcPath, dstPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dataURL, err := neturl.JoinPath(url, srcPath)
	if err != nil {
		return err
	}

	req, _ := http.NewRequest(http.MethodGet, dataURL, nil)

	resp, err := sClient.HTTPDo(req)
	if err != nil {
		return fmt.Errorf("HTTPDo: %s", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.ContentLength > 0 {
			msg, err := io.ReadAll(io.LimitReader(resp.Body, 1000))
			if err == nil {
				return fmt.Errorf("Backend response \"%s\" Msg: %s", resp.Status, string(msg))
			}
		}

		return fmt.Errorf("Backend response \"%s\"", resp.Status)
	}

	if srcPath != "" && srcPath[len(srcPath)-1:] == "/" {
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			firstErr error
		)

		// Keep only the first non-nil sub-download error.
		setFirstErr := func(subPath string, subErr error) {
			if subErr == nil {
				return
			}

			mu.Lock()

			if firstErr == nil {
				firstErr = fmt.Errorf("download %s: %w", filepath.Join(srcPath, subPath), subErr)
			}

			mu.Unlock()
		}

		downloadOne := func(subPath string) {
			subErr := RecursiveDownload(ctx, sClient, log, sem, url, srcPath+subPath, filepath.Join(dstPath, subPath))
			setFirstErr(subPath, subErr)
		}

		err = forRespItems(resp.Body, func(item *dirItem) error {
			subPath := item.Name
			switch item.Type {
			case itemTypeDir:
				err = os.MkdirAll(filepath.Join(dstPath, subPath), os.ModePerm)
				if err != nil {
					return fmt.Errorf("Create dir error: %s", err.Error())
				}

				subPath += "/"
			case itemTypeFile, itemTypeLink:
				// downloadable, proceed below
			default:
				log.Warn("Skipping unsupported entry during filesystem download", slog.String("path", item.Name), slog.String("type", item.Type))
				return nil
			}

			// Run subtask in a goroutine when semaphore capacity is available;
			// otherwise process inline to avoid blocking on sem (prevents deadlock on wide trees).
			select {
			case sem <- struct{}{}:
				wg.Add(1)

				go func(sp string) {
					defer func() {
						<-sem
						wg.Done()
					}()

					downloadOne(sp)
				}(subPath)
			default:
				downloadOne(subPath)
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("Response body (%s) error: %s", srcPath, err.Error())
		}

		wg.Wait()

		return firstErr
	}

	if dstPath != "" {
		// Create out file
		out, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer out.Close()

		_, err = io.Copy(out, resp.Body)
		if err != nil {
			return err
		}

		log.Info("Downloaded file", slog.String("path", dstPath))
	} else {
		_, err = io.Copy(os.Stdout, resp.Body)
		if err != nil {
			return err
		}
	}

	return nil
}
