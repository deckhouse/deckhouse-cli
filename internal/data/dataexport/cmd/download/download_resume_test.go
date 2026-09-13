package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	"github.com/deckhouse/deckhouse-cli/internal/dataplane"
	safereq "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// --- fixtures -------------------------------------------------------------

// stubExport points the command at srv instead of a cluster: PrepareDownload
// normally creates a DataExport and waits for its exporter pod.
func stubExport(t *testing.T, baseURL, volumeMode string) {
	t.Helper()

	origPrep := util.PrepareDownloadFunc
	origCreate := util.CreateDataExporterIfNeededFunc

	util.PrepareDownloadFunc = func(_ context.Context, _ *slog.Logger, _ dataapi.Backend, _, _ string, _ bool, _ *safereq.SafeClient) (string, string, *safereq.SafeClient, error) {
		return baseURL, volumeMode, newNoAuthSafe(), nil
	}
	util.CreateDataExporterIfNeededFunc = func(_ context.Context, _ *slog.Logger, de, _ string, _ bool, _ string, _ ctrlclient.Client) (string, error) {
		return de, nil
	}

	t.Cleanup(func() {
		util.PrepareDownloadFunc = origPrep
		util.CreateDataExporterIfNeededFunc = origCreate
	})
}

// clockScale is how much faster the retry clock runs in tests than in
// production. The production backoff starts at one second, so a test surviving
// two breaks would otherwise spend three seconds asleep.
const clockScale = 1000

// fastRetries runs one test under the PRODUCTION policy with only its clock
// scaled down: the bounds that decide how many attempts a transfer gets —
// Backoff.Steps and MaxNoProgress — are taken from DefaultRetryPolicy and never
// restated here, so a change to them reaches these tests instead of leaving them
// asserting against a number the command no longer uses.
//
// Duration and Cap are divided by the same factor on purpose. Cap does double
// duty in wait.Backoff: besides capping one sleep it ends the loop early once the
// next delay would exceed it, and that is decided by the RATIO of the two, which
// dividing both leaves untouched. Jitter is dropped so the arithmetic is the same
// on every run.
func fastRetries(t *testing.T) {
	t.Helper()

	policy := dataplane.DefaultRetryPolicy()
	policy.Backoff.Duration /= clockScale
	policy.Backoff.Cap /= clockScale
	policy.Backoff.Jitter = 0

	orig := downloadRetryPolicy
	downloadRetryPolicy = policy

	t.Cleanup(func() { downloadRetryPolicy = orig })
}

// syncBuffer collects log output from the goroutines a tree download spawns.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// captureLog returns a logger admitting exactly what the command's production
// logger admits (internal/data/dataexport/util.SetupLogger builds an INFO-level
// text handler). A guard on evidence the operator is supposed to read must not
// accept a line that only appears when the level is turned down.
func captureLog(t *testing.T) (*slog.Logger, *syncBuffer) {
	t.Helper()

	buf := &syncBuffer{}

	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

// retryLines returns the lines the shared retry loop emits when it survives a
// break. Reading them out of the command's own output is the only way to tell a
// transfer that survived a break from one no break ever reached — a recovered
// transfer looks exactly like an untroubled one everywhere else — so the test
// reads them the same way an operator has to.
func retryLines(output string) []string {
	var found []string

	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "retrying") {
			found = append(found, line)
		}
	}

	return found
}

// pseudoRandom builds test content that no misplaced resume can accidentally
// reproduce: a repeating pattern would make a body written at the wrong offset
// compare equal to the source, which is the one failure the byte-for-byte
// assertions exist to catch.
func pseudoRandom(n int, seed int64) []byte {
	b := make([]byte, n)
	//nolint:gosec // deterministic test content, not a security control
	r := rand.New(rand.NewSource(seed))
	_, _ = r.Read(b)

	return b
}

func requireSameBytes(t *testing.T, want, got []byte) {
	t.Helper()

	require.Len(t, got, len(want), "downloaded length differs from source length")

	for i := range want {
		if want[i] != got[i] {
			require.Failf(t, "downloaded content differs from source",
				"first difference at offset %d: want %#x, got %#x", i, want[i], got[i])
		}
	}
}

// objectStub serves one object over the data-plane contract (a plain GET for the
// whole object, "Range: bytes=N-" plus 206 to continue from N) and kills the
// connection part way through the requests named by breaks.
//
// breaks[i] is how many bytes request i delivers before the connection dies;
// requests past the end of breaks are served to the end. alwaysBreak, when
// positive, makes EVERY request die after that many bytes.
type objectStub struct {
	src         []byte
	breaks      []int
	alwaysBreak int

	// refuse, when non-zero, answers every request with that status and an
	// explanation instead of the object.
	refuse int

	mu     sync.Mutex
	ranges []string
}

// requestRanges returns the Range header of every request received, in order.
func (o *objectStub) requestRanges() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]string(nil), o.ranges...)
}

func (o *objectStub) serve(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	index := len(o.ranges)
	o.ranges = append(o.ranges, r.Header.Get("Range"))
	o.mu.Unlock()

	if o.refuse > 0 {
		http.Error(w, "refused by the exporter", o.refuse)

		return
	}

	start, err := rangeStart(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	total := int64(len(o.src))
	if start > total {
		http.Error(w, "range beyond object", http.StatusRequestedRangeNotSatisfiable)

		return
	}

	remaining := total - start

	deliver := remaining

	switch {
	case o.alwaysBreak > 0:
		deliver = min(int64(o.alwaysBreak), remaining)
	case index < len(o.breaks):
		deliver = min(int64(o.breaks[index]), remaining)
	}

	w.Header().Set("Content-Length", strconv.FormatInt(remaining, 10))

	if start > 0 {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, total-1, total))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	_, _ = w.Write(o.src[start : start+deliver])

	if deliver < remaining {
		// Flush first: the bytes have to reach the client, or the break is
		// indistinguishable from a request that delivered nothing at all.
		w.(http.Flusher).Flush()

		// The documented way for a handler to drop the connection without the
		// server logging a stack trace. The client sees the body end short of
		// the Content-Length it was promised.
		panic(http.ErrAbortHandler)
	}
}

// rangeStart reads the "bytes=N-" form this command sends. Anything else is a
// request the stub refuses to guess at.
func rangeStart(r *http.Request) (int64, error) {
	header := r.Header.Get("Range")
	if header == "" {
		return 0, nil
	}

	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok || !strings.HasSuffix(spec, "-") {
		return 0, fmt.Errorf("unsupported Range header %q", header)
	}

	return strconv.ParseInt(strings.TrimSuffix(spec, "-"), 10, 64)
}

// listingJSON renders a directory listing the way the exporter does, in a fixed
// order: a fixture whose order changes from run to run decides by coin flip which
// entry a client reaches first, and every assertion about what happens after a
// failure goes with it.
func listingJSON(dir string, files map[string]*objectStub) string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}

	sort.Strings(names)

	items := make([]string, 0, len(files))

	for _, name := range names {
		items = append(items, fmt.Sprintf(
			`{"name":%q,"type":"file","uri":%q,"attributes":{"gid":0,"uid":0,"permissions":"0644","modtime":"2026-01-01T00:00:00Z","size":%d}}`,
			name, dir+"/"+name, len(files[name].src)))
	}

	return fmt.Sprintf(`{"apiVersion":"v1","items":[%s]}`, strings.Join(items, ","))
}

// treeServer serves a one-level filesystem export: a listing at "/api/v1/files/<dir>/"
// and one objectStub per file under it.
func treeServer(t *testing.T, dir string, files map[string]*objectStub) *httptest.Server {
	t.Helper()

	listingPath := "/api/v1/files/" + dir + "/"

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == listingPath {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, listingJSON(dir, files))

			return
		}

		name := strings.TrimPrefix(r.URL.Path, listingPath)

		obj, ok := files[name]
		if !ok {
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusInternalServerError)

			return
		}

		obj.serve(w, r)
	}))
}

// runDownload executes the command against a stubbed export and returns its error
// and its log output.
func runDownload(t *testing.T, args []string, log *slog.Logger) error {
	t.Helper()

	cmd := NewCommand(context.TODO(), log)
	cmd.SetArgs(append(args, "--publish=false"))
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	return cmd.Execute()
}

// --- INVARIANT 1 and 5: resume from the reached offset, byte for byte --------

func TestDownloadBlock_ResumesFromReachedOffsetAndMatchesSource(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4096, 7)
	stub := &objectStub{src: src, breaks: []int{1500, 1100}}

	srv := httptest.NewServer(http.HandlerFunc(stub.serve))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/block", "Block")

	out := filepath.Join(t.TempDir(), "raw.img")

	log, _ := captureLog(t)
	require.NoError(t, runDownload(t, []string{"myexport", "-o", out}, log))

	got, err := os.ReadFile(out)
	require.NoError(t, err)
	requireSameBytes(t, src, got)

	// Two breaks, two resumes, each asking for exactly the bytes already on
	// disk onwards: 1500 delivered, then 1100 more.
	require.Equal(t, []string{"", "bytes=1500-", "bytes=2600-"}, stub.requestRanges())
}

func TestDownloadFilesystem_ResumesFileOfTreeAndMatchesSource(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4096, 11)
	stub := &objectStub{src: src, breaks: []int{900}}

	srv := treeServer(t, "root", map[string]*objectStub{"big.bin": stub})
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/files", "Filesystem")

	outDir := t.TempDir()

	log, logs := captureLog(t)
	require.NoError(t, runDownload(t, []string{"myexport", "root/", "-o", outDir}, log))

	got, err := os.ReadFile(filepath.Join(outDir, "big.bin"))
	require.NoError(t, err)
	requireSameBytes(t, src, got)

	require.Equal(t, []string{"", "bytes=900-"}, stub.requestRanges())

	// The break is attributed to the file it happened in, which is what makes a
	// tree's evidence readable at all: one connection carries many files.
	lines := retryLines(logs.String())
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "transfer=root/big.bin")
	require.Contains(t, lines[0], "resume_offset=900")
}

// --- INVARIANT 1, the substitution: a server that ignores Range ---------------

func TestDownloadBlock_RefusesToAppendWhenServerIgnoresRange(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4096, 13)

	var requests int

	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		first := requests == 1
		mu.Unlock()

		// Answers 200 with the whole object however the request was ranged —
		// the shape of a proxy or an exporter without range support.
		w.Header().Set("Content-Length", strconv.Itoa(len(src)))
		w.WriteHeader(http.StatusOK)

		if first {
			_, _ = w.Write(src[:1500])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}

		_, _ = w.Write(src)
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/block", "Block")

	out := filepath.Join(t.TempDir(), "raw.img")

	log, _ := captureLog(t)
	err := runDownload(t, []string{"myexport", "-o", out}, log)
	require.ErrorIs(t, err, dataplane.ErrRangeIgnored)

	// The file must hold what actually arrived and nothing more: appending a
	// from-zero body behind the first 1500 bytes is the corruption this refuses.
	got, readErr := os.ReadFile(out)
	require.NoError(t, readErr)
	requireSameBytes(t, src[:1500], got)
}

// --- INVARIANT 2: a stream that ends cleanly short of the declared length -----

func TestDownloadBlock_FailsWhenStreamEndsShortOfDeclaredLength(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4096, 17)

	var requests int

	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		first := requests == 1
		mu.Unlock()

		if first {
			w.Header().Set("Content-Length", strconv.Itoa(len(src)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(src[:1500])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}

		// Declares the whole object in Content-Range, then ends the body
		// cleanly after 1000 of the 2596 bytes left. No Content-Length and an
		// explicit flush make it a chunked response, so the transport has
		// nothing to notice: to io.Copy this is an ordinary completed stream.
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 1500-%d/%d", len(src)-1, len(src)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(src[1500:2500])
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/block", "Block")

	out := filepath.Join(t.TempDir(), "raw.img")

	log, _ := captureLog(t)
	err := runDownload(t, []string{"myexport", "-o", out}, log)
	require.Error(t, err)
	require.ErrorContains(t, err, "delivered 2500 bytes, server declared 4096")

	// Two requests, not a whole budget of them: a producer contradicting its own
	// header will contradict it again, so this ends the transfer at once.
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, requests)
}

// --- INVARIANT 3: a failed transfer leaves a non-zero exit status -------------

func TestDownloadBlock_FailsWhenBreaksNeverStop(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(8192, 19)

	// Delivers bytes on every attempt and dies every time: the shape that a
	// per-attempt "made progress, so reset the budget" rule would ride forever.
	stub := &objectStub{src: src, alwaysBreak: 100}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The stub answers every request, however many come: the count is
		// checked below, and refusing service after N would hand a retrying
		// client a different failure to stop on.
		stub.serve(w, r)
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/block", "Block")

	out := filepath.Join(t.TempDir(), "raw.img")

	log, _ := captureLog(t)

	// Bounded in TIME, not only in retries, and that is the point of running the
	// command on another goroutine. A retry loop that never gives up does not
	// fail an assertion — it never reaches one, and a guard that waits for the
	// loop to end goes down with it, taking the whole package to its timeout.
	done := make(chan error, 1)
	go func() { done <- runDownload(t, []string{"myexport", "-o", out}, log) }()

	var err error

	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the command never stopped retrying a link that keeps breaking")
	}

	require.Error(t, err)
	require.ErrorContains(t, err, "block volume")

	// It gave up because the budget ran out, not on the first break: a fatal
	// verdict on attempt one would satisfy "an error came back" just as well,
	// and would mean this test had stopped covering the budget at all.
	require.ErrorContains(t, err, "exhausted")
	require.Greater(t, len(stub.requestRanges()), 1, "the transfer must be retried before it gives up")
	// The bound comes from the production policy itself, not from a number
	// copied next to the assertion: a loop that reset its budget on every
	// attempt that delivered bytes would sail past it, and a policy edited later
	// moves this assertion with it.
	require.LessOrEqual(t, len(stub.requestRanges()), dataplane.DefaultRetryPolicy().Backoff.Steps,
		"the number of attempts must stay inside the policy budget")
}

// --- INVARIANT 4: one broken file does not cancel the rest of the tree --------

func TestDownloadFilesystem_BrokenFileDoesNotCancelSiblings(t *testing.T) {
	fastRetries(t)

	good := pseudoRandom(2048, 23)
	other := pseudoRandom(3000, 29)
	doomed := pseudoRandom(4096, 31)

	files := map[string]*objectStub{
		"good.bin":   {src: good},
		"other.bin":  {src: other, breaks: []int{700}},
		"doomed.bin": {src: doomed, alwaysBreak: 64},
	}

	srv := treeServer(t, "root", files)
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/files", "Filesystem")

	outDir := t.TempDir()

	log, _ := captureLog(t)
	err := runDownload(t, []string{"myexport", "root/", "-o", outDir}, log)
	require.Error(t, err)
	require.ErrorContains(t, err, "doomed.bin", "the failure must name the file that did not arrive")

	// The siblings still arrived, complete and intact — including the one that
	// survived a break of its own.
	got, readErr := os.ReadFile(filepath.Join(outDir, "good.bin"))
	require.NoError(t, readErr)
	requireSameBytes(t, good, got)

	got, readErr = os.ReadFile(filepath.Join(outDir, "other.bin"))
	require.NoError(t, readErr)
	requireSameBytes(t, other, got)
}

// --- observability: the command's own output shows the break was survived -----

func TestDownloadBlock_ReportsEveryRecoveredBreakAtDefaultLevel(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4096, 37)
	stub := &objectStub{src: src, breaks: []int{1500, 1100}}

	srv := httptest.NewServer(http.HandlerFunc(stub.serve))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/block", "Block")

	out := filepath.Join(t.TempDir(), "raw.img")

	log, logs := captureLog(t)
	require.NoError(t, runDownload(t, []string{"myexport", "-o", out}, log))

	// Two breaks, two lines: without the attempt number a run that survived one
	// break reads exactly like a run that survived fifteen.
	lines := retryLines(logs.String())
	require.Len(t, lines, 2)

	require.Contains(t, lines[0], `transfer="block volume"`)
	require.Contains(t, lines[0], "attempt=1")
	require.Contains(t, lines[0], "resume_offset=1500")

	// The second line carries three different numbers on purpose: the attempt
	// number, the bytes this attempt delivered and the offset the next one
	// resumes from are all distinct, so a line reporting one of them in place of
	// another cannot satisfy this.
	require.Contains(t, lines[1], "attempt=2")
	require.Contains(t, lines[1], "delivered_bytes=1100")
	require.Contains(t, lines[1], "resume_offset=2600")
}

// --- listing entries may not steer the download out of the destination -------

func TestDownloadFilesystem_RejectsListingEntryEscapingDestination(t *testing.T) {
	fastRetries(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/files/root/" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"apiVersion":"v1","items":[` +
				`{"name":"../escaped.bin","type":"file","uri":"root/../escaped.bin","attributes":{"size":3}}` +
				`]}`))

			return
		}

		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("own"))
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/files", "Filesystem")

	parent := t.TempDir()
	outDir := filepath.Join(parent, "out")
	require.NoError(t, os.MkdirAll(outDir, 0o755))

	log, _ := captureLog(t)
	err := runDownload(t, []string{"myexport", "root/", "-o", outDir}, log)
	require.Error(t, err)
	require.ErrorContains(t, err, "not a single directory entry name")

	_, statErr := os.Stat(filepath.Join(parent, "escaped.bin"))
	require.True(t, os.IsNotExist(statErr), "nothing may be written outside the destination directory")
}

// A producer whose declared length changes between attempts is no longer serving
// the object the bytes on disk came from, and the resume would splice two
// different objects into one file. The transfer stops where it stands instead.
func TestDownloadBlock_StopsWhenDeclaredLengthChangesBetweenAttempts(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(8192, 41)

	var requests int

	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		first := requests == 1
		mu.Unlock()

		if first {
			// Declares 4096 and breaks after 1500 of them.
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(src[:1500])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}

		// The resume is answered for an object of a different size.
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 1500-%d/%d", len(src)-1, len(src)))
		w.Header().Set("Content-Length", strconv.Itoa(len(src)-1500))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(src[1500:])
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/block", "Block")

	out := filepath.Join(t.TempDir(), "raw.img")

	log, _ := captureLog(t)
	err := runDownload(t, []string{"myexport", "-o", out}, log)
	require.Error(t, err)
	require.ErrorContains(t, err, "first declared 4096 bytes and now declares 8192")

	// Not one byte of the other object reached the file.
	got, readErr := os.ReadFile(out)
	require.NoError(t, readErr)
	requireSameBytes(t, src[:1500], got)
}

// An entry with an empty name resolves back to the directory that listed it, so a
// client that accepts it walks into the same directory forever, spawning a
// goroutine and opening a connection every time round.
func TestDownloadFilesystem_RejectsEmptyListingEntryName(t *testing.T) {
	fastRetries(t)

	var mu sync.Mutex

	listings := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/") {
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusInternalServerError)

			return
		}

		mu.Lock()
		listings++
		count := listings
		mu.Unlock()

		// The stub bounds the walk itself. A test that waits for an endless
		// recursion to finish never reports anything: it hangs until the whole
		// package times out, which is not a red test.
		if count > 5 {
			http.Error(w, "client walked back into the same directory", http.StatusInternalServerError)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// The VALID entry in front of the empty one is load-bearing, not
		// decoration. It makes the attempt report a delivery before it fails,
		// which is what a retryable failure looks like — so this fixture also
		// holds the refusal to its second property: a listing this client will
		// not walk is refused ONCE, not re-fetched until the budget runs out.
		// Drop the first entry and the assertion below passes either way.
		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
			`{"name":"ok.bin","type":"file","uri":"root/ok.bin","attributes":{"size":3}},`+
			`{"name":"","type":"dir","uri":"root/","attributes":{}}`+
			`]}`)
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/files", "Filesystem")

	log, _ := captureLog(t)
	err := runDownload(t, []string{"myexport", "root/", "-o", t.TempDir()}, log)
	require.Error(t, err)
	require.ErrorContains(t, err, "not a single directory entry name")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, listings, "the directory must be listed once: neither walked into again, nor re-fetched")
}

// The listing is a leg of the transfer like any other, and the break that this
// client exists to survive kills every connection at once — the one carrying the
// listing included. Losing it must not cost the whole tree.
func TestDownloadFilesystem_SurvivesABreakInTheListing(t *testing.T) {
	fastRetries(t)

	first := pseudoRandom(600, 47)
	second := pseudoRandom(700, 53)

	files := map[string]*objectStub{
		"a.bin": {src: first},
		"b.bin": {src: second},
	}

	listing := listingJSON("root", files)

	var mu sync.Mutex

	listings := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/files/root/" {
			mu.Lock()
			listings++
			broken := listings == 1
			mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			if broken {
				// Cut AFTER the first entry, not at half the body. Where the cut
				// falls decides what this test is holding on to: break before any
				// entry is decoded and the attempt reports no delivery, so the
				// retry happens only because the classifier recognizes a broken
				// HTTP/1.1 body by name — which the break this client exists to
				// survive is not (see TestListDir_RetriesAnUnnamedBreakOnceEntriesHaveArrived).
				_, _ = io.WriteString(w, listing[:afterFirstEntry(t, listing)])
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			}

			_, _ = io.WriteString(w, listing)

			return
		}

		name := strings.TrimPrefix(r.URL.Path, "/api/v1/files/root/")

		obj, ok := files[name]
		if !ok {
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusInternalServerError)

			return
		}

		obj.serve(w, r)
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/files", "Filesystem")

	outDir := t.TempDir()

	log, logs := captureLog(t)
	require.NoError(t, runDownload(t, []string{"myexport", "root/", "-o", outDir}, log))

	got, err := os.ReadFile(filepath.Join(outDir, "a.bin"))
	require.NoError(t, err)
	requireSameBytes(t, first, got)

	got, err = os.ReadFile(filepath.Join(outDir, "b.bin"))
	require.NoError(t, err)
	requireSameBytes(t, second, got)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, listings, "the listing must be asked for again after it broke")

	// The survived break is reported for the listing leg as it is for a file leg,
	// so a run that recovered here is distinguishable from one that never broke.
	lines := retryLines(logs.String())
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], `transfer="listing root/"`)
}

// A refused request must leave whatever is already at the output path alone:
// truncating it destroys an earlier download before a single byte of the new one
// has been shown to exist.
func TestDownloadBlock_LeavesExistingDestinationIntactWhenBackendRefuses(t *testing.T) {
	fastRetries(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "VolumeMode: Filesystem. Not supported downloading raw block.", http.StatusBadRequest)
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/block", "Block")

	out := filepath.Join(t.TempDir(), "raw.img")
	previous := []byte("an earlier download that must survive a refused request")
	require.NoError(t, os.WriteFile(out, previous, 0o600))

	log, _ := captureLog(t)
	err := runDownload(t, []string{"myexport", "-o", out}, log)
	require.Error(t, err)

	got, readErr := os.ReadFile(out)
	require.NoError(t, readErr)
	requireSameBytes(t, previous, got)
}

// A transfer can lose its connection AFTER the last byte and before the stream
// ends. Everything the producer declared is then on disk, and asking for "the
// rest" means asking for a range that starts past the end of the object — which a
// producer answers with 416, turning an arrived transfer into a loud failure.
func TestDownloadBlock_AcceptsATransferBrokenAfterTheLastByte(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4096, 43)

	var mu sync.Mutex

	requests := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		count := requests
		mu.Unlock()

		switch count {
		case 1:
			w.Header().Set("Content-Length", strconv.Itoa(len(src)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(src[:1500])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		case 2:
			// Every remaining byte arrives, and then the connection dies before
			// the chunked stream is terminated: the client holds the whole
			// object and an error.
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 1500-%d/%d", len(src)-1, len(src)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(src[1500:])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		default:
			// What a producer serving through http.ServeContent answers to a
			// range starting at or past the end of the object.
			start, err := rangeStart(r)
			require.NoError(t, err)
			require.GreaterOrEqual(t, start, int64(len(src)))

			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(src)))
			http.Error(w, "range beyond object", http.StatusRequestedRangeNotSatisfiable)
		}
	}))
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/block", "Block")

	out := filepath.Join(t.TempDir(), "raw.img")

	log, _ := captureLog(t)
	require.NoError(t, runDownload(t, []string{"myexport", "-o", out}, log))

	got, err := os.ReadFile(out)
	require.NoError(t, err)
	requireSameBytes(t, src, got)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, requests, "a transfer already holding every declared byte must not ask for more")
}

// --- the listing leg's retry, on the break shape the stand produces -----------

// errCarrierLost stands in for a transport failure no classifier can name, which
// is what the break this client exists to survive actually is: an HTTP/2 session
// torn down mid-body surfaces as a type net/http keeps inside its own unexported
// copy of the http2 package, unreachable to errors.As from here.
//
// It cannot be produced through httptest, and that is why these two tests drive
// the Fetcher directly. A body broken over HTTP/1.1 arrives as
// io.ErrUnexpectedEOF, which the shared classifier recognizes BY NAME and retries
// whether or not the attempt delivered anything — so a test driven through
// httptest says nothing about the progress rule the listing leg depends on.
var errCarrierLost = errors.New("carrier lost")

// afterFirstEntry returns the offset just past the first entry of a listing, so a
// truncated body can be cut where exactly one entry has been decoded.
func afterFirstEntry(t *testing.T, listing string) int {
	t.Helper()

	end := strings.Index(listing, "},")
	require.Greater(t, end, 0, "listing fixture must hold more than one entry")

	return end + len("},")
}

// bodyEndingWith delivers data and then fails with err instead of ending cleanly
// — a connection that carried bytes and then died, rather than a stream that
// finished.
type bodyEndingWith struct {
	data []byte
	err  error
	pos  int
}

func (b *bodyEndingWith) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, b.err
	}

	n := copy(p, b.data[b.pos:])
	b.pos += n

	return n, nil
}

func (b *bodyEndingWith) Close() error { return nil }

// listingDownloader builds a downloader whose transport answers every listing
// request from bodies, so a test can hand it a break of a shape httptest cannot
// produce.
func listingDownloader(t *testing.T, log *slog.Logger, bodies func(request int) io.ReadCloser) (*downloader, func() int) {
	t.Helper()

	var mu sync.Mutex

	requests := 0

	doer := dataplane.DoerFunc(func(_ *http.Request) (*http.Response, error) {
		mu.Lock()
		requests++
		count := requests
		mu.Unlock()

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       bodies(count),
		}, nil
	})

	d := &downloader{
		fetcher: dataplane.NewFetcher(doer),
		retrier: dataplane.NewRetrier(downloadPolicy()),
		log:     log,
		sem:     make(chan struct{}, maxParallelFiles),
	}

	return d, func() int {
		mu.Lock()
		defer mu.Unlock()

		return requests
	}
}

// The listing leg earns its retry from the entries it decoded, not from the name
// of the error that ended it. This is the guard on that: the break carries an
// error nothing classifies, so the only thing that can buy a second attempt is
// the delivery the attempt reported.
func TestListDir_RetriesAnUnnamedBreakOnceEntriesHaveArrived(t *testing.T) {
	fastRetries(t)

	listing := listingJSON("root", map[string]*objectStub{
		"a.bin": {src: []byte("a")},
		"b.bin": {src: []byte("bb")},
	})
	cut := afterFirstEntry(t, listing)

	log, logs := captureLog(t)

	d, requests := listingDownloader(t, log, func(request int) io.ReadCloser {
		if request == 1 {
			return &bodyEndingWith{data: []byte(listing[:cut]), err: errCarrierLost}
		}

		return io.NopCloser(strings.NewReader(listing))
	})

	entries, err := d.listDir(context.Background(), "http://exporter.invalid/api/v1/files/root/", "/root/")
	require.NoError(t, err)

	// Exactly the two entries, each once: a retry re-reads the listing from the
	// start, so an attempt that kept what the broken one had decoded would leave
	// the first entry here twice.
	require.Equal(t, []listedEntry{
		{name: "a.bin", itemType: itemTypeFile},
		{name: "b.bin", itemType: itemTypeFile},
	}, entries)

	require.Equal(t, 2, requests())
	require.Len(t, retryLines(logs.String()), 1)
}

// The declared limit of that retry, held to its word: a break that arrives before
// the first entry reports no delivery, so a failure nothing classifies ends the
// walk there instead of being retried. It is the shared policy's own rule for a
// stream that broke before delivering a byte, and this leg is not special enough
// to earn an exception — if that is ever reconsidered, this test says so, and the
// limit named in listDir's doc comment has to move with it.
func TestListDir_DoesNotRetryAnUnnamedBreakBeforeTheFirstEntry(t *testing.T) {
	fastRetries(t)

	listing := listingJSON("root", map[string]*objectStub{"a.bin": {src: []byte("a")}})

	beforeAnyEntry := strings.Index(listing, "[") + 1
	require.Greater(t, beforeAnyEntry, 1)

	log, _ := captureLog(t)

	d, requests := listingDownloader(t, log, func(int) io.ReadCloser {
		return &bodyEndingWith{data: []byte(listing[:beforeAnyEntry]), err: errCarrierLost}
	})

	entries, err := d.listDir(context.Background(), "http://exporter.invalid/api/v1/files/root/", "/root/")
	require.Error(t, err)
	require.ErrorIs(t, err, errCarrierLost)
	require.Nil(t, entries)
	require.Equal(t, 1, requests(), "an attempt that delivered nothing must not buy another one")
}

// An entry the producer refuses must leave nothing behind at its destination. The
// destination of a tree download is routinely a directory holding an earlier copy
// of the same tree, and a file created before the first answer arrives replaces
// the earlier copy of that file with an empty one.
func TestDownloadFilesystem_RefusedEntryLeavesNoFileBehind(t *testing.T) {
	fastRetries(t)

	wanted := pseudoRandom(800, 59)

	files := map[string]*objectStub{
		"a-good.bin":    {src: wanted},
		"b-refused.bin": {src: []byte("never served"), refuse: http.StatusBadRequest},
	}

	srv := treeServer(t, "root", files)
	defer srv.Close()

	stubExport(t, srv.URL+"/api/v1/files", "Filesystem")

	outDir := t.TempDir()

	log, _ := captureLog(t)
	err := runDownload(t, []string{"myexport", "root/", "-o", outDir}, log)
	require.Error(t, err)
	require.ErrorContains(t, err, "b-refused.bin")
	require.ErrorContains(t, err, "refused by the exporter")

	got, readErr := os.ReadFile(filepath.Join(outDir, "a-good.bin"))
	require.NoError(t, readErr)
	requireSameBytes(t, wanted, got)

	_, statErr := os.Stat(filepath.Join(outDir, "b-refused.bin"))
	require.True(t, os.IsNotExist(statErr), "a refused entry must not leave a file at its destination")
}
