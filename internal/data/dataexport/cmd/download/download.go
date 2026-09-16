/*
Copyright 2024 Flant JSC

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

package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	"github.com/deckhouse/deckhouse-cli/internal/dataplane"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdName = "download"
)

const (
	itemTypeDir  = "dir"
	itemTypeFile = "file"
	itemTypeLink = "link"
)

// maxParallelFiles bounds how many files of a tree are fetched at once.
const maxParallelFiles = 10

// blockSubject names the single stream of a Block-mode download in retry log
// lines and errors, where a Filesystem download names the file instead.
const blockSubject = "block volume"

// errStreamLengthMismatch reports a body that ended CLEANLY at a different length
// than the producer declared for it.
var errStreamLengthMismatch = errors.New("stream length differs from the length the server declared")

// errBadListingEntry reports a listing entry whose name is not one directory
// entry, and so names nothing the producer can serve.
var errBadListingEntry = errors.New("listing entry name is not a single directory entry")

// downloadRetryPolicy is the retry policy every stream of this command runs
// under. It is a package variable for one reason only: the production backoff
// starts at one second, so a test exercising several breaks would spend nearly
// all its runtime asleep. Tests replace it and restore it; nothing else writes it.
var downloadRetryPolicy = dataplane.DefaultRetryPolicy()

func cmdExamples() string {
	resp := []string{
		"  # Start exporter + Download + Stop for Filesystem",
		fmt.Sprintf("    ... %s [flags] kind/volume_name path/file.ext [-o out_file.ext]", cmdName),
		fmt.Sprintf("    ... %s -n target-namespace pvc/my-file-volume mydir/testdir/file.txt -o file.txt", cmdName),
		"  # Start exporter + Download + Stop for Block",
		fmt.Sprintf("    ... %s [flags] kind/volume_name [-o out_file.ext]", cmdName),
		fmt.Sprintf("    ... %s -n target-namespace vs/my-vs-volume -o file.txt", cmdName),
		"  # Start exporter + Download + Stop for VirtualDisk (Block)",
		fmt.Sprintf("    ... %s -n target-namespace vd/my-virtualdisk -o file.img", cmdName),
		"  # Start exporter + Download + Stop for VirtualDiskSnapshot (Block)",
		fmt.Sprintf("    ... %s -n target-namespace vds/my-virtualdisk-snapshot -o file.img", cmdName),
	}

	return strings.Join(resp, "\n")
}

func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] [KIND/]data_export_name [path/file.ext]",
		Short:   "Download exported data",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(ctx, log, cmd, args)
		},
		Args: func(_ *cobra.Command, args []string) error {
			_, _, err := dataio.ParseArgs(args)
			return err
		},
	}

	cmd.Flags().StringP("namespace", "n", dataio.Namespace, "data volume namespace")
	cmd.Flags().StringP("output", "o", "", "file to save data (default: same as resource)") // TODO support /dev/stdout
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")
	cmd.Flags().String("ttl", "2m", "Time to live for auto-created DataExport")
	cmd.Flags().Bool("cleanup", false, "Delete auto-created DataExport without prompting (--cleanup=true to delete, --cleanup=false to keep)")

	return cmd
}

// downloadPolicy returns the retry policy for one run: the shared data-plane
// policy plus the two judgements this command adds to it.
//
// errStreamLengthMismatch is fatal because a body that ended CLEANLY short of the
// declared length is the producer's own framing saying "that was all" while its
// own header said otherwise, and asking again gets the same disagreement back. A
// link that actually breaks never arrives this way — Go's transport reports a
// truncated body as io.ErrUnexpectedEOF, which the shared policy retries.
//
// errBadListingEntry is fatal because a listing entry the client refuses to walk
// is a property of what the producer sends, not of the link it came over: the
// same listing would be refused on every attempt, and without this it would cost
// a whole budget of re-listings to find that out.
func downloadPolicy() dataplane.RetryPolicy {
	policy := downloadRetryPolicy

	// Extend, never replace: assigning here would silently drop whatever the
	// shared policy had already named fatal.
	inherited := policy.Fatal
	policy.Fatal = func(err error) bool {
		return errors.Is(err, errStreamLengthMismatch) ||
			errors.Is(err, errBadListingEntry) ||
			(inherited != nil && inherited(err))
	}

	return policy
}

// downloader carries what every stream of one run shares. One instance per run:
// the Fetcher and Retrier inside it are safe for the concurrent file downloads a
// tree spawns.
type downloader struct {
	fetcher *dataplane.Fetcher
	retrier *dataplane.Retrier
	log     *slog.Logger
	sem     chan struct{}
}

// downloadPath fetches whatever srcPath names under baseURL into dstPath: a
// directory tree when srcPath ends in "/", a single stream otherwise. Block-mode
// downloads arrive here with an empty srcPath and take the stream branch.
func (d *downloader) downloadPath(ctx context.Context, baseURL, srcPath, dstPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if strings.HasSuffix(srcPath, "/") {
		return d.downloadDir(ctx, baseURL, srcPath, dstPath)
	}

	fileURL, err := neturl.JoinPath(baseURL, srcPath)
	if err != nil {
		return err
	}

	subject := strings.TrimPrefix(srcPath, "/")
	if subject == "" {
		subject = blockSubject
	}

	return d.downloadFile(ctx, fileURL, dstPath, subject)
}

// listedEntry is what one listing entry contributes to the walk: a name and what
// kind of thing it is. Only these two are kept, so the memory one buffered
// listing costs stays close to the length of its names (see listDir's LIMIT).
type listedEntry struct {
	name     string
	itemType string
}

// listDir reads one directory's listing to the end BEFORE anything under it is
// downloaded, and retries the whole listing under the shared policy when the
// transport breaks in the middle of it.
//
// Reading it to the end first is what makes the retry possible at all. Streaming
// the listing straight into the downloads — decode an entry, fetch it, decode the
// next — holds the listing's own connection open for the entire tree transfer,
// and the break this client exists to survive kills every connection at once, the
// listing's included. Losing it that way costs the whole tree, and a retry cannot
// help: entries already decoded have already spawned downloads, so re-reading the
// listing would start a second download of the same file into the same path.
// Buffered, an attempt has no effect outside its own slice, so a retry simply
// asks for the listing again and throws away what the broken attempt decoded.
//
// PROGRESS COORDINATES. The attempt reports the ENTRIES it decoded, not bytes:
// "the caller's own coordinates" is what dataplane.Progress asks for, and entries
// decoded is what says this attempt's connection carried data before it broke —
// the observable the shared policy needs to grant another attempt for a transport
// failure no classifier can name (an HTTP/2 session torn down mid-body carries a
// type no errors.As from here can reach). The two numbers the retry line prints
// for this leg are therefore per-attempt entry counts, and its resume offset says
// where the NEXT attempt starts only in the sense that it starts over.
//
// LIMIT of buffering, stated plainly: the listing of one directory is held in
// memory while that directory is walked — a name and a type per entry, so on the
// order of a hundred bytes each, and tens of megabytes for a directory of a
// million entries. The shared decoder streams precisely so that ITS memory stays
// independent of entry count, and this command gives that up for the directory it
// is walking. More than one can be alive at a time: the semaphore caps concurrent
// entries at maxParallelFiles, and inline recursion adds one listing per level of
// depth.
//
// LIMIT of the retry: an attempt that breaks before it decodes its first entry
// reports no delivery, so a transport failure no classifier names ends the walk
// there. That is the shared policy's own rule for a stream that broke before
// delivering a byte, and this leg is not special enough to earn an exception.
func (d *downloader) listDir(ctx context.Context, dirURL, srcPath string) ([]listedEntry, error) {
	var entries []listedEntry

	attempt := func(ctx context.Context) (dataplane.Progress, error) {
		// A retry starts the listing over: whatever the broken attempt decoded
		// is discarded here rather than appended to.
		entries = entries[:0]

		err := d.fetcher.ListDir(ctx, dirURL, func(item dataplane.Item) error {
			if nameErr := checkEntryName(srcPath, item.Name); nameErr != nil {
				return nameErr
			}

			entries = append(entries, listedEntry{name: item.Name, itemType: item.Type})

			return nil
		})

		return dataplane.Progress{Start: 0, Durable: int64(len(entries))}, err
	}

	if err := d.retrier.Resume(ctx, d.log, "listing "+strings.TrimPrefix(srcPath, "/"), attempt); err != nil {
		return nil, fmt.Errorf("Response body (%s) error: %w", srcPath, err)
	}

	return entries, nil
}

// checkEntryName rejects a listing entry whose name is not one directory entry.
//
// The producer builds Name from a single directory entry, so a name carrying a
// separator, naming the parent, or EMPTY describes nothing it can serve. Each of
// the three does its own damage: a separator (or "..") would place the download
// outside dstPath once filepath.Join gets hold of it, and an empty name resolves
// back to the directory that listed it, so a client that accepts it walks into
// the same directory again — forever, spawning a goroutine and opening a
// connection every time round.
//
// "/" is refused everywhere because it is the separator on the wire;
// filepath.Separator adds "\" on the platforms whose filesystem treats it as one,
// and leaves it the ordinary filename character it is on the others.
func checkEntryName(srcPath, name string) error {
	if name == "" || name == "." || name == ".." ||
		strings.Contains(name, "/") || strings.ContainsRune(name, filepath.Separator) {
		return fmt.Errorf("%w: listing %s returned %q, which is not a single directory entry name",
			errBadListingEntry, srcPath, name)
	}

	return nil
}

// downloadDir downloads the entries of one directory, spawning a goroutine per
// entry while the semaphore has room and processing inline otherwise (which keeps
// a wide tree from deadlocking on the semaphore).
//
// A failure in one entry does not cancel its siblings: the first error is kept
// and returned once every entry has finished, so a tree download reports what did
// not arrive instead of dropping the files that would still have arrived.
func (d *downloader) downloadDir(ctx context.Context, baseURL, srcPath, dstPath string) error {
	dirURL, err := neturl.JoinPath(baseURL, srcPath)
	if err != nil {
		return err
	}

	entries, err := d.listDir(ctx, dirURL, srcPath)
	if err != nil {
		return err
	}

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
		subErr := d.downloadPath(ctx, baseURL, srcPath+subPath, filepath.Join(dstPath, subPath))
		setFirstErr(subPath, subErr)
	}

	for _, entry := range entries {
		subPath := entry.name

		switch entry.itemType {
		case itemTypeDir:
			if err := os.MkdirAll(filepath.Join(dstPath, subPath), os.ModePerm); err != nil {
				setFirstErr(subPath, fmt.Errorf("Create dir error: %s", err.Error()))

				continue
			}

			subPath += "/"
		case itemTypeFile, itemTypeLink:
			// downloadable, proceed below
		default:
			d.log.Warn("Skipping unsupported entry during filesystem download", slog.String("path", entry.name), slog.String("type", entry.itemType))

			continue
		}

		// Run subtask in a goroutine when semaphore capacity is available;
		// otherwise process inline to avoid blocking on sem (prevents deadlock on wide trees).
		select {
		case d.sem <- struct{}{}:
			wg.Add(1)

			go func(sp string) {
				defer func() {
					<-d.sem
					wg.Done()
				}()

				downloadOne(sp)
			}(subPath)
		default:
			downloadOne(subPath)
		}
	}

	wg.Wait()

	return firstErr
}

// downloadFile streams one object — the block volume, or one regular file of a
// tree — into dstPath, resuming from the bytes already written whenever the
// transport breaks mid-body. The retry budget is per file, so one file exhausting
// it fails that file rather than the files being fetched alongside it.
func (d *downloader) downloadFile(ctx context.Context, fileURL, dstPath, subject string) error {
	stream := &fileStream{
		fetcher:  d.fetcher,
		url:      fileURL,
		dstPath:  dstPath,
		subject:  subject,
		expected: -1,
	}

	resumeErr := d.retrier.Resume(ctx, d.log, subject, stream.attempt)

	// Close reports write failures a filesystem only discovers on flush, so it is
	// checked rather than deferred away — but after the transfer's own error,
	// which describes the failure better.
	closeErr := stream.close()

	switch {
	case resumeErr != nil:
		return resumeErr
	case closeErr != nil:
		return fmt.Errorf("close %s: %w", dstPath, closeErr)
	}

	d.log.Info("Downloaded file", slog.String("path", dstPath))

	return nil
}

// fileStream is one object's transfer: how far it has got, and what the producer
// says the whole object's length is. One instance belongs to one downloadFile
// call and its attempts run one after another in that call's goroutine, so its
// fields need no synchronization.
type fileStream struct {
	fetcher *dataplane.Fetcher
	url     string
	dstPath string
	subject string

	// out is opened by the FIRST answer the producer gives, never before it. The
	// destination of a download is routinely the result of an earlier one, and
	// creating it up front truncates that earlier download before a single byte
	// of the new one has been shown to exist: a refused request would destroy it,
	// and a tree would leave an empty file behind for every entry the producer
	// refuses (TestDownloadBlock_LeavesExistingDestinationIntactWhenBackendRefuses
	// and TestDownloadFilesystem_RefusedEntryLeavesNoFileBehind hold both).
	out *os.File

	// written counts the bytes the DESTINATION accepted, never the bytes read
	// off the wire. The two part company exactly when a write fails part way
	// through, and it is the written count that says where the next attempt has
	// to continue from: a resume offset taken from the read side leaves a hole
	// of the unwritten bytes inside a file of exactly the right length.
	//
	// Asking the destination for its size instead would be the other candidate,
	// and it is wrong here — "-o /dev/sdb" is a legitimate destination, and a
	// block device reports size 0 however much has been written into it.
	written int64

	// expected is the length the producer declared for the WHOLE object, or -1
	// while no response has declared one.
	expected int64
}

// attempt performs one try of the transfer: it asks for everything from the
// durable offset onwards and appends what arrives. It never retries internally —
// the retry seam is Retrier.Resume.
func (s *fileStream) attempt(ctx context.Context) (dataplane.Progress, error) {
	start := s.written

	// Every early return reports Start == Durable, so an attempt that failed
	// before delivering anything reads as a zero-delivery attempt rather than as
	// the whole durable prefix it inherited.
	resumed := dataplane.Progress{Start: start, Durable: start}

	if s.expected >= 0 && s.written >= s.expected {
		// Every byte the producer declared is already on disk: the previous
		// attempt lost its connection AFTER the last byte and before the stream
		// ended (a session torn down before the terminator, or the idle
		// watchdog firing while waiting for it). Asking for "the rest" here
		// would ask for a range starting past the end of the object, which a
		// producer serving through http.ServeContent answers with 416 — a loud
		// failure for a transfer that in fact arrived.
		return resumed, nil
	}

	body, declared, err := s.fetcher.OpenStream(ctx, s.url, start)
	if err != nil {
		return resumed, err
	}

	defer func() { _ = body.Close() }()

	if err := s.noteDeclared(declared); err != nil {
		return resumed, err
	}

	if err := s.open(); err != nil {
		return resumed, err
	}

	// Position the write explicitly at the offset just requested from the
	// producer rather than trusting the handle to be where the previous attempt
	// left it: those two agreeing is what keeps resumed bytes from landing at
	// the wrong offset.
	if _, err := s.out.Seek(start, io.SeekStart); err != nil {
		return resumed, fmt.Errorf("seek %s to offset %d: %w", s.subject, start, err)
	}

	n, copyErr := io.Copy(s.out, body)
	s.written = start + n
	progress := dataplane.Progress{Start: start, Durable: s.written}

	if copyErr != nil {
		return progress, fmt.Errorf("stream %s: %w", s.subject, copyErr)
	}

	return progress, s.checkLength()
}

// open creates the destination, once, on the first answer that got as far as a
// body. Later attempts reuse the same handle: reopening with O_TRUNC would throw
// away the prefix the transfer is resuming from.
func (s *fileStream) open() error {
	if s.out != nil {
		return nil
	}

	out, err := os.Create(s.dstPath)
	if err != nil {
		return err
	}

	s.out = out

	return nil
}

// close closes the destination if the transfer ever got far enough to open it.
func (s *fileStream) close() error {
	if s.out == nil {
		return nil
	}

	return s.out.Close()
}

// noteDeclared records the length the producer declared for the whole object, and
// rejects a producer that declares a different one on a later attempt: the object
// behind the URL changed between attempts, so the bytes already on disk and the
// bytes about to be appended are no longer parts of the same object.
func (s *fileStream) noteDeclared(declared int64) error {
	switch {
	case declared < 0:
		return nil
	case s.expected < 0:
		s.expected = declared

		return nil
	case s.expected != declared:
		return fmt.Errorf("%s first declared %d bytes and now declares %d", s.subject, s.expected, declared)
	default:
		return nil
	}
}

// checkLength compares what arrived with what the producer declared, and is the
// only thing between a truncated transfer and a report of success: a body that
// ends cleanly is, to io.Copy, an ordinary completed stream.
//
// LIMIT, and it is not hypothetical: a producer that declares no length at all (a
// chunked response) leaves nothing to compare against, and a transfer cut short
// by such a producer passes unnoticed. Neither exporter sends one for a file or
// for the block device — both serve through http.ServeContent, which always
// declares a length — so what is left uncovered is a proxy rewriting the
// response, not the servers this command talks to.
func (s *fileStream) checkLength() error {
	if s.expected < 0 || s.written == s.expected {
		return nil
	}

	return fmt.Errorf("%w: %s delivered %d bytes, server declared %d",
		errStreamLengthMismatch, s.subject, s.written, s.expected)
}

func Run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, args []string) error {
	namespace, _ := cmd.Flags().GetString("namespace")
	dstPath, _ := cmd.Flags().GetString("output")
	ttl, _ := cmd.Flags().GetString("ttl")
	cleanup, _ := cmd.Flags().GetBool("cleanup")
	cleanupExplicit := cmd.Flags().Changed("cleanup")

	dataName, srcPath, err := dataio.ParseArgs(args)
	if err != nil {
		return fmt.Errorf("arguments parsing error: %s", err.Error())
	}

	flags := cmd.PersistentFlags()
	safeClient.SupportNoAuth = false

	sClient, err := safeClient.NewSafeClient(flags)
	if err != nil {
		return err
	}

	backend, rtClient, err := util.ResolveClientFunc(ctx, sClient, namespace, log)
	if err != nil {
		return err
	}

	publishFlag, err := dataio.ParsePublishFlag(cmd.Flags())
	if err != nil {
		return err
	}

	publish, err := dataio.ResolvePublish(ctx, publishFlag, rtClient, sClient, log)
	if err != nil {
		return err
	}

	deName, err := util.CreateDataExporterIfNeededFunc(ctx, log, dataName, namespace, publish, ttl, rtClient)
	if err != nil {
		return err
	}

	log.Info("DataExport created", slog.String("name", deName), slog.String("namespace", namespace))

	url, volumeMode, subClient, err := util.PrepareDownloadFunc(ctx, log, backend, deName, namespace, publish, sClient)
	if err != nil {
		return err
	}

	switch volumeMode {
	case "Filesystem":
		if srcPath == "" {
			return fmt.Errorf("invalid source path: '%s'", srcPath)
		}

		if dstPath == "" {
			pathList := strings.Split(srcPath, "/")
			dstPath = pathList[len(pathList)-1]
		}
	case "Block":
		srcPath = ""

		if dstPath == "" {
			dstPath = deName
		}
	default:
		return fmt.Errorf("%w: %s", dataio.ErrUnsupportedVolumeMode, volumeMode)
	}

	log.Info("Start downloading", slog.String("url", url+srcPath), slog.String("dstPath", dstPath))

	transfer := &downloader{
		// SafeClient's only request method is HTTPDo, so it reaches the typed
		// data-plane client through the adapter rather than directly.
		fetcher: dataplane.NewFetcher(dataplane.DoerFunc(subClient.HTTPDo)),
		retrier: dataplane.NewRetrier(downloadPolicy()),
		log:     log,
		sem:     make(chan struct{}, maxParallelFiles),
	}

	downloadErr := transfer.downloadPath(ctx, url, srcPath, dstPath)
	if downloadErr != nil {
		log.Error("Not all files have been downloaded", slog.String("error", downloadErr.Error()))
	} else {
		log.Info("All files have been downloaded", slog.String("dst_path", dstPath))
	}

	// Clean up auto-created DataExport
	if deName != dataName && dataio.ShouldCleanup(cleanup, cleanupExplicit) {
		if err := util.DeleteDataExport(ctx, deName, namespace, rtClient); err != nil {
			log.Warn("Failed to delete DataExport", slog.String("name", deName), slog.String("error", err.Error()))
		}
	}

	// A failed transfer has to leave a non-zero exit status behind it: cmd/d8
	// turns any error out of RunE into exit code 1. It is returned after the
	// cleanup above, so a failure never leaks an auto-created DataExport.
	return downloadErr
}
