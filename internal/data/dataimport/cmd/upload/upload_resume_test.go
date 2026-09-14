package upload

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	"github.com/deckhouse/deckhouse-cli/internal/dataplane"
	safereq "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// --- fixtures -------------------------------------------------------------

// dstPath is where every test in this file uploads to. The importer stub serves
// exactly this one destination, so a request for any other path is a client that
// built the wrong URL rather than a case the stub forgot.
const dstPath = "/dst/payload.bin"

// dataSubpath is the endpoint PrepareUpload hands back for a Filesystem import;
// the command joins the destination path onto it.
const dataSubpath = "/api/v1/files"

// newNoAuthSafe returns a SafeClient that talks plain HTTP to an httptest server
// instead of to a cluster.
func newNoAuthSafe(t *testing.T) *safereq.SafeClient {
	t.Helper()

	safereq.SupportNoAuth = true

	oldKubeconfig := os.Getenv("KUBECONFIG")
	require.NoError(t, os.Setenv("KUBECONFIG", "/dev/null"))

	defer func() { _ = os.Setenv("KUBECONFIG", oldKubeconfig) }()

	sc, err := safereq.NewSafeClient()
	require.NoError(t, err)

	return sc.Copy()
}

// stubImport points the command at srv instead of a cluster: the two steps it
// replaces resolve the API group and wait for a DataImport to be reconciled and
// its importer pod to come up.
func stubImport(t *testing.T, baseURL string) {
	t.Helper()

	origResolve := util.ResolveClientFunc
	origPrepare := util.PrepareUploadFunc

	util.ResolveClientFunc = func(context.Context, *safereq.SafeClient, string, *slog.Logger) (dataapi.Backend, ctrlclient.Client, error) {
		return dataapi.Backend{}, nil, nil
	}
	util.PrepareUploadFunc = func(context.Context, dataapi.Backend, string, string, bool, *safereq.SafeClient, *slog.Logger) (string, string, string, *safereq.SafeClient, error) {
		return baseURL + dataSubpath, baseURL, "Filesystem", newNoAuthSafe(t), nil
	}

	t.Cleanup(func() {
		util.ResolveClientFunc = origResolve
		util.PrepareUploadFunc = origPrepare
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
// duty in wait.Backoff: besides capping one sleep it ends the loop early once
// the next delay would exceed it, and that is decided by the RATIO of the two,
// which dividing both leaves untouched. Jitter is dropped so the arithmetic is
// the same on every run.
func fastRetries(t *testing.T) {
	t.Helper()

	policy := dataplane.DefaultRetryPolicy()
	policy.Backoff.Duration /= clockScale
	policy.Backoff.Cap /= clockScale
	policy.Backoff.Jitter = 0

	orig := uploadRetryPolicy
	uploadRetryPolicy = policy

	t.Cleanup(func() { uploadRetryPolicy = orig })
}

// captureLog returns a logger admitting exactly what the command's production
// logger admits: `d8 data import` hands its subcommands slog.Default(), whose
// handler admits INFO and above. A guard on evidence the operator is supposed to
// read must not accept a line that only appears when the level is turned down.
func captureLog() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}

	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

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
// reproduce: a repeating pattern would make bytes written at the wrong offset
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

	require.Len(t, got, len(want), "uploaded length differs from source length")

	for i := range want {
		if want[i] != got[i] {
			require.Failf(t, "uploaded content differs from source",
				"first difference at offset %d: want %#x, got %#x", i, want[i], got[i])
		}
	}
}

// --- the importer stub ----------------------------------------------------

// conflict describes a 409 the stub answers with instead of accepting a chunk.
type conflict struct {
	// expected, when non-empty, is sent as X-Expected-Offset. Empty means the
	// header is omitted — the answer both importers give while a previous,
	// broken handler is still draining, and the one the file importer gives
	// for a file it already holds in full.
	expected string
}

// importer is the upload half of the data-plane contract as the FILESYSTEM
// importer serves it: it accumulates the bytes a client PUTs at X-Offset,
// reports where it stands on HEAD, and records the finalisation POST.
//
// Which of the two importers it models matters in one visible place. The
// filesystem one acknowledges the write that completes the file with 201 and no
// X-Next-Offset, having just renamed the partial upload onto the destination,
// and names the offset only on the writes before that; the block one names an
// offset on every answer it gives. This stub follows the filesystem shape, so
// the header-less acknowledgement is exercised by every test here that runs to
// completion rather than only by the one that aims at it.
//
// It also bounds ITSELF. A client that keeps retrying forever must fail this
// test by tripping maxPuts, not by running until the package timeout takes the
// whole suite down with it — a guard that hangs on the implementation it is
// meant to catch cannot be shown to fail.
type importer struct {
	t     *testing.T
	total int64

	// breakAfter maps a 1-based PUT number to the number of body bytes the
	// stub accepts before killing the connection.
	breakAfter map[int]int

	// conflicts maps a 1-based PUT number to the 409 answered instead.
	conflicts map[int]conflict

	// headSize, when set, makes HEAD answer with that Content-Length and NO
	// resume offset, whatever the stub has actually accepted. That is what the
	// filesystem importer answers when the destination file is present and no
	// partial upload of it is — including when the file present is not the one
	// this run is uploading, and so is not the declared size.
	headSize *int64

	// headStatus is the status those answers carry, 200 when unset. A 404 with
	// a length is not something an importer sends, but the client's rule for
	// one is written down and so is worth pinning.
	headStatus int

	// acknowledgeWithoutMoving makes every PUT answer 200 while naming back the
	// very offset it was sent at, which is an importer that acknowledges and
	// accepts nothing.
	acknowledgeWithoutMoving bool

	// rewindOn maps a 1-based PUT number to an offset the importer drops back
	// to before refusing that PUT, naming the offset in X-Expected-Offset. It
	// stands for an importer that lost part of what it had acknowledged, which
	// is the only shape where obeying the header and ignoring it differ
	// observably.
	rewindOn map[int]int64

	// preloaded seeds the accepted prefix, standing in for what a previous run
	// of the command left behind.
	preloaded []byte

	maxPuts int

	mu         sync.Mutex
	accepted   []byte
	putOffsets []int64
	heads      int
	finished   int
	loaded     bool
}

func newImporter(t *testing.T, total int64) *importer {
	t.Helper()

	return &importer{
		t:          t,
		total:      total,
		breakAfter: map[int]int{},
		conflicts:  map[int]conflict{},
		rewindOn:   map[int]int64{},
		maxPuts:    32,
	}
}

func (im *importer) start() *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(im.serve))
	im.t.Cleanup(srv.Close)

	return srv
}

func (im *importer) snapshot() []byte {
	im.mu.Lock()
	defer im.mu.Unlock()

	return append([]byte(nil), im.accepted...)
}

func (im *importer) offsets() []int64 {
	im.mu.Lock()
	defer im.mu.Unlock()

	return append([]int64(nil), im.putOffsets...)
}

func (im *importer) finishedCount() int {
	im.mu.Lock()
	defer im.mu.Unlock()

	return im.finished
}

func (im *importer) headCount() int {
	im.mu.Lock()
	defer im.mu.Unlock()

	return im.heads
}

func (im *importer) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/v1/finished" {
		im.mu.Lock()
		im.finished++
		im.mu.Unlock()
		w.WriteHeader(http.StatusOK)

		return
	}

	if r.URL.Path != dataSubpath+dstPath {
		http.Error(w, "unexpected path: "+r.URL.Path, http.StatusInternalServerError)

		return
	}

	switch r.Method {
	case http.MethodHead:
		im.serveHead(w)
	case http.MethodPut:
		im.servePut(w, r)
	default:
		http.Error(w, "unexpected method: "+r.Method, http.StatusInternalServerError)
	}
}

// serveHead answers the way the importer does: a partly filled destination gets
// its resume offset named, a full one gets its size and NO offset at all, and a
// destination nothing has been written to yet does not exist.
func (im *importer) serveHead(w http.ResponseWriter) {
	im.mu.Lock()
	im.heads++
	im.ensureLoadedLocked()
	held := int64(len(im.accepted))
	stale := im.headSize
	status := im.headStatus
	im.mu.Unlock()

	if stale != nil {
		if status == 0 {
			status = http.StatusOK
		}

		w.Header().Set("Content-Length", strconv.FormatInt(*stale, 10))
		w.WriteHeader(status)

		return
	}

	if held == 0 {
		http.Error(w, "no such destination", http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(im.total, 10))

	if held < im.total {
		w.Header().Set("X-Next-Offset", strconv.FormatInt(held, 10))
	}

	w.WriteHeader(http.StatusOK)
}

func (im *importer) servePut(w http.ResponseWriter, r *http.Request) {
	im.mu.Lock()
	im.ensureLoadedLocked()

	offset, err := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
	if err != nil {
		im.mu.Unlock()
		http.Error(w, "bad X-Offset", http.StatusBadRequest)

		return
	}

	im.putOffsets = append(im.putOffsets, offset)
	put := len(im.putOffsets)

	if put > im.maxPuts {
		im.mu.Unlock()
		im.t.Errorf("client issued more than %d PUTs; it is not bounding its retries", im.maxPuts)
		http.Error(w, "too many attempts", http.StatusInternalServerError)

		return
	}

	if rewind, ok := im.rewindOn[put]; ok {
		im.accepted = im.accepted[:rewind]
		im.mu.Unlock()
		w.Header().Set("X-Expected-Offset", strconv.FormatInt(rewind, 10))
		w.WriteHeader(http.StatusConflict)

		return
	}

	forced, forcedOK := im.conflicts[put]
	kill, killOK := im.breakAfter[put]
	held := int64(len(im.accepted))
	stuck := im.acknowledgeWithoutMoving
	im.mu.Unlock()

	if stuck {
		w.Header().Set("X-Next-Offset", strconv.FormatInt(offset, 10))
		w.WriteHeader(http.StatusOK)

		return
	}

	if forcedOK {
		if forced.expected != "" {
			w.Header().Set("X-Expected-Offset", forced.expected)
		}

		w.WriteHeader(http.StatusConflict)

		return
	}

	// The real importer refuses a write that does not continue its own prefix,
	// and names where it stands.
	if offset != held {
		w.Header().Set("X-Expected-Offset", strconv.FormatInt(held, 10))
		w.WriteHeader(http.StatusConflict)

		return
	}

	body, readErr := io.ReadAll(r.Body)
	if readErr != nil && !killOK {
		http.Error(w, "read body: "+readErr.Error(), http.StatusInternalServerError)

		return
	}

	if killOK {
		if kill > len(body) {
			kill = len(body)
		}

		body = body[:kill]
	}

	im.mu.Lock()
	im.accepted = append(im.accepted, body...)
	held = int64(len(im.accepted))
	im.mu.Unlock()

	if killOK {
		// Kill the connection after the bytes have been taken, which is what a
		// reloaded ingress does to an upload in flight: the importer keeps what
		// it got and the client never learns how much that was.
		panic(http.ErrAbortHandler)
	}

	if held >= im.total {
		// The write that completes the destination: the importer renames the
		// partial upload onto it and acknowledges without naming an offset,
		// there being nothing left to resume from.
		w.WriteHeader(http.StatusCreated)

		return
	}

	w.Header().Set("X-Next-Offset", strconv.FormatInt(held, 10))
	w.WriteHeader(http.StatusNoContent)
}

func (im *importer) ensureLoadedLocked() {
	if im.loaded {
		return
	}

	im.loaded = true
	im.accepted = append(im.accepted, im.preloaded...)
}

// --- the runner -----------------------------------------------------------

// runUpload writes src to a temp file and runs the command against srv.
func runUpload(t *testing.T, srv *httptest.Server, src []byte, log *slog.Logger, extra ...string) error {
	t.Helper()

	stubImport(t, srv.URL)

	path := filepath.Join(t.TempDir(), "payload.bin")
	require.NoError(t, os.WriteFile(path, src, 0o600))

	args := append([]string{
		"di-name",
		"-n", "ns",
		"--publish=false",
		"-d", dstPath,
		"-f", path,
	}, extra...)

	cmd := NewCommand(context.TODO(), log)
	cmd.SetArgs(args)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	return cmd.Execute()
}

// --- INVARIANT 1 and byte-for-byte equality --------------------------------

// TestUpload_ResumesFromImporterOffsetAfterBreakAndMatchesSource holds the
// centre of this block: a connection dying mid-transfer must not end the
// command, and what the importer ends up holding must equal the source byte for
// byte.
//
// The stub keeps the bytes it took before dying and never tells the client how
// many those were, so a client that resumed from its own optimistic count would
// leave a hole here and a client that restarted would duplicate a prefix — and
// both are caught by the comparison rather than by a length check, which is why
// the payload is pseudo-random.
func TestUpload_ResumesFromImporterOffsetAfterBreakAndMatchesSource(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(40_000, 1)

	im := newImporter(t, int64(len(src)))
	im.breakAfter[2] = 3_000
	im.breakAfter[4] = 1_500

	srv := im.start()

	log, out := captureLog()
	require.NoError(t, runUpload(t, srv, src, log, "-c", "5"))

	requireSameBytes(t, src, im.snapshot())
	require.Equal(t, 1, im.finishedCount(), "a completed upload must be finalised exactly once")

	// Every PUT must start where the importer stood, never lower: re-sending
	// bytes the importer already has is what a restart looks like.
	offsets := im.offsets()
	require.GreaterOrEqual(t, len(offsets), 7, "two breaks must have cost extra PUTs")

	for i := 1; i < len(offsets); i++ {
		require.GreaterOrEqual(t, offsets[i], offsets[i-1],
			"PUT %d went backwards, from offset %d to %d", i, offsets[i-1], offsets[i])
	}

	// INVARIANT of observability: the break has to be visible in THIS command's
	// output, with the attempt number and the offset the retry continued from.
	lines := retryLines(out.String())
	require.Len(t, lines, 2, "each survived break must print exactly one retry line")

	for _, line := range lines {
		require.Contains(t, line, "attempt=", "a retry line without an attempt number cannot tell one break from fifteen")
		require.Contains(t, line, "resume_offset=", "a retry line without an offset cannot show the retry resumed rather than restarted")
	}

	require.Contains(t, lines[0], "attempt=1")
	require.Contains(t, lines[1], "attempt=2")
}

// --- INVARIANT 1: the importer's offset wins over the client's --------------

// TestUpload_AdoptsExpectedOffsetFromConflict holds that a 409 naming
// X-Expected-Offset moves the client to that offset instead of the client
// continuing from its own idea of where it stands.
//
// The conflict here names an offset BELOW what the client believes, which is the
// only shape that separates the two: a client ignoring the header would carry on
// from its own offset, the importer would refuse every following chunk, and the
// transfer would never complete.
func TestUpload_AdoptsExpectedOffsetFromConflict(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(20_000, 2)
	chunk := int64(len(src) / 4)

	im := newImporter(t, int64(len(src)))
	// After two chunks are acknowledged, the importer drops back to one and
	// says so. A client that carried on from its own offset would be refused
	// from here on and would never finish; a client that obeys the header
	// re-sends chunk two and completes.
	im.rewindOn[3] = chunk

	srv := im.start()

	log, _ := captureLog()
	require.NoError(t, runUpload(t, srv, src, log, "-c", "4"))

	requireSameBytes(t, src, im.snapshot())
	require.Equal(t, 1, im.finishedCount())

	offsets := im.offsets()
	require.Len(t, offsets, 6, "the rewind must cost exactly two extra PUTs")
	require.Equal(t, []int64{0, chunk, 2 * chunk, chunk, 2 * chunk, 3 * chunk}, offsets,
		"the PUT after the conflict must start at the offset the importer named")
}

// --- INVARIANT 1b: a conflict is a question, not a verdict -----------------

// chunkPositions runs a case at both chunk counts that matter, because an
// importer refusing WITHOUT naming an offset decides before it looks at the
// offset at all: it refuses the first chunk of ten exactly as it refuses a lone
// one. A guard written only at "-c 1" puts the refusal on the request that would
// have finished the transfer, which is the one position where any rule keyed on
// reaching the end is satisfied for free — and a run with the default ten chunks
// meets the opposite position on its very first request.
var chunkPositions = []struct {
	name string
	args []string
}{
	{name: "refused on the request that would have finished it", args: []string{"-c", "1"}},
	{name: "refused on a request that is not the last", args: nil},
}

// TestUpload_ConflictWithoutHeaderOnFullDestinationSucceeds is the positive half
// of a pair, and the two halves differ in EXACTLY ONE thing: how much of the
// file the importer already holds. Everything else — a conflict with no
// X-Expected-Offset answered to every PUT, the chunk count, the payload — is
// identical, so nothing but the answer to "how far has it got" can separate
// them. Anything that made this one pass while the other also passed would be
// reading the conflict as a verdict, which is what the pair is here to forbid.
//
// This half is the transfer that ARRIVED: the last PUT landed, its answer was
// lost, the run died before it could finalise, and the next run meets the reply
// the filesystem importer gives for a file it already holds in full.
func TestUpload_ConflictWithoutHeaderOnFullDestinationSucceeds(t *testing.T) {
	for _, position := range chunkPositions {
		t.Run(position.name, func(t *testing.T) {
			fastRetries(t)

			src := pseudoRandom(8_000, 3)

			im := newImporter(t, int64(len(src)))
			// Everything is already there: this run follows one whose last
			// acknowledgement never came back.
			im.preloaded = src

			for put := 1; put <= im.maxPuts; put++ {
				im.conflicts[put] = conflict{}
			}

			srv := im.start()

			log, _ := captureLog()
			require.NoError(t, runUpload(t, srv, src, log, position.args...),
				"a conflict without an offset on a destination the importer already holds in full is not a failure")

			requireSameBytes(t, src, im.snapshot())
			require.Equal(t, 1, im.finishedCount(), "an upload the importer already holds must still be finalised")
			require.Positive(t, im.headCount(), "the conflict must have been resolved by asking, not by assuming")
		})
	}
}

// TestUpload_ConflictWithoutHeaderOnPartialDestinationIsNotSuccess is the
// negative half, and without it the positive one above is green for the
// implementation "a conflict without a header means the destination is full" —
// which finalises an import over a half-written volume.
//
// The importer holds a FRACTION of the file and is otherwise set up exactly as
// in the positive half, at both chunk positions.
func TestUpload_ConflictWithoutHeaderOnPartialDestinationIsNotSuccess(t *testing.T) {
	for _, position := range chunkPositions {
		t.Run(position.name, func(t *testing.T) {
			fastRetries(t)

			src := pseudoRandom(8_000, 4)

			im := newImporter(t, int64(len(src)))
			im.preloaded = src[:2_000]

			for put := 1; put <= im.maxPuts; put++ {
				im.conflicts[put] = conflict{}
			}

			srv := im.start()

			log, _ := captureLog()
			err := runUpload(t, srv, src, log, position.args...)

			require.Error(t, err, "a conflict without an offset on a partly written destination is not completion")
			require.Zero(t, im.finishedCount(), "a transfer that did not complete must not be finalised")
			require.Len(t, im.snapshot(), 2_000, "nothing may have been added to the destination")
		})
	}
}

// TestUpload_UnreadableExpectedOffsetIsRefused holds the branch that reads the
// importer's stated position. A header that cannot be read is not a position,
// and the tempting failure is to let it decay into zero — which re-uploads the
// whole file over a destination that already holds a prefix of it.
//
// The bad header lands on ONE request and the importer is healthy either side of
// it, which is what makes the failure observable at all: a client that let the
// unreadable value become zero would be sent back on course by the importer's
// next, well-formed conflict and would finish the upload, so the guard is the
// difference between this run failing and this run succeeding. An importer that
// refused everything would fail the run under both readings and prove nothing.
//
// The empty header is deliberately NOT in this table: an absent X-Expected-Offset
// is the header-less conflict, a different branch with its own pair of guards
// above.
func TestUpload_UnreadableExpectedOffsetIsRefused(t *testing.T) {
	for _, header := range []string{"soon", "-1", "9e9"} {
		t.Run("X-Expected-Offset "+strconv.Quote(header), func(t *testing.T) {
			fastRetries(t)

			src := pseudoRandom(6_000, 12)

			im := newImporter(t, int64(len(src)))
			im.preloaded = src[:3_000]
			im.conflicts[1] = conflict{expected: header}

			srv := im.start()

			log, _ := captureLog()
			err := runUpload(t, srv, src, log, "-c", "2")

			require.Error(t, err, "an offset the client cannot read is not an offset to continue from")
			require.ErrorContains(t, err, header, "the failure must name the header it could not read")
			require.Zero(t, im.finishedCount())
			require.Len(t, im.snapshot(), 3_000, "nothing may have been written after an unreadable answer")
		})
	}
}

// TestUpload_ConflictNamingTheSameOffsetIsBounded holds the other new branch:
// an importer that refuses a chunk and names back the very offset the chunk
// began at. Obeying that literally means sending the same chunk to the same
// refusal forever, so it has to be handed to the retry loop, which bounds it.
//
// The stub bounds itself as well, so an unbounded client fails this by tripping
// maxPuts rather than by hanging until the package timeout.
func TestUpload_ConflictNamingTheSameOffsetIsBounded(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(6_000, 13)

	im := newImporter(t, int64(len(src)))

	for put := 1; put <= im.maxPuts; put++ {
		im.conflicts[put] = conflict{expected: "0"}
	}

	srv := im.start()

	log, _ := captureLog()
	err := runUpload(t, srv, src, log, "-c", "2")

	require.Error(t, err, "an importer that keeps naming the same offset must end the command, not spin it")
	require.Zero(t, im.finishedCount())
	require.LessOrEqual(t, len(im.offsets()), im.maxPuts)
}

// TestUpload_DestinationOfTheWrongSizeIsNotComplete holds the size test that
// decides whether an answer naming no resume offset means "finished".
//
// The other guards never reach it: an importer holding a PARTIAL upload names
// its offset, so the check that an offset was named settles those cases before
// size is consulted. The case that gets here is a destination the importer
// considers settled at a size that is not the one this run declared — a file
// already sitting at that path, left by something other than this upload. Read
// as finished it finalises an import over the wrong bytes, at the wrong length,
// with no error anywhere.
func TestUpload_DestinationOfTheWrongSizeIsNotComplete(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(6_000, 14)

	stale := int64(3_000)

	im := newImporter(t, int64(len(src)))
	im.headSize = &stale

	for put := 1; put <= im.maxPuts; put++ {
		im.breakAfter[put] = 500
	}

	srv := im.start()

	log, _ := captureLog()
	err := runUpload(t, srv, src, log, "-c", "1")

	require.Error(t, err, "a destination settled at a size this run never declared is not a finished upload")
	require.Zero(t, im.finishedCount(), "an import must not be finalised over a destination of the wrong length")
}

// --- INVARIANT 2 and 3: no finalisation, non-zero exit ---------------------

// TestUpload_ExhaustedBudgetFailsWithoutFinalising holds both bounds at once on
// the shape that is hardest to bound: every attempt delivers bytes and then
// breaks, so the consecutive-no-progress ceiling is reset each time round and
// only the backoff's step budget stands between the loop and running forever.
//
// The stub therefore bounds itself (maxPuts). An implementation that resets the
// budget on progress would otherwise not turn this guard red — it would hang it,
// and take the package's whole test run with it.
// TestUpload_OffsetPastTheEndIsNotSuccess pins the one direction the loop in
// attempt cannot notice by itself. Every other refusal leaves the offset short
// of totalSize and the loop keeps going; an offset PAST the end satisfies the
// loop's own exit condition, so the run ends having sent nothing and with
// nothing left to object.
//
// The importer does not have to misbehave to produce one. It names the size of
// the partial file it holds at the destination path, and that size belongs to
// whatever ran there before — so a previous, larger upload interrupted at the
// same path makes it name an offset this run can never reach. Observed live
// against the filesystem importer: X-Content-Length 52428800 answered with
// 409 X-Expected-Offset 55364826.
//
// The guard sits in run(), after the retry loop, so it covers every writer of
// the offset and not only the conflict header exercised here.
func TestUpload_OffsetPastTheEndIsNotSuccess(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4_096, 21)

	im := newImporter(t, int64(len(src)))
	im.conflicts[1] = conflict{expected: "999999999"}

	srv := im.start()

	log, _ := captureLog()
	err := runUpload(t, srv, src, log, "-c", "1")

	require.Error(t, err, "an offset past the end of the source is not a finished upload")
	require.Zero(t, im.finishedCount(), "an import must not be finalised over bytes this run never sent")
	require.Empty(t, im.snapshot(), "not a byte of the source belongs at the destination in this state")
}

func TestUpload_ExhaustedBudgetFailsWithoutFinalising(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(60_000, 5)

	im := newImporter(t, int64(len(src)))
	for put := 1; put <= im.maxPuts; put++ {
		im.breakAfter[put] = 300
	}

	srv := im.start()

	log, out := captureLog()
	err := runUpload(t, srv, src, log, "-c", "1")

	require.Error(t, err, "a transfer that never finishes must leave a non-zero exit code behind")
	require.Zero(t, im.finishedCount(), "an unfinished transfer must not be finalised")
	require.Less(t, len(im.snapshot()), len(src), "the test is only meaningful while the destination stays short")

	// Every attempt delivered bytes, so the guard is on the step budget rather
	// than on the no-progress ceiling.
	require.NotEmpty(t, retryLines(out.String()), "the attempts that delivered and broke must be visible")
}

// --- INVARIANT 2: finalisation is tied to completeness ---------------------

// TestUpload_RefusedChunkFailsWithoutFinalising is the plain shape of the same
// rule: the importer refuses outright, the command reports it, and nothing is
// finalised.
func TestUpload_RefusedChunkFailsWithoutFinalising(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4_000, 6)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/finished" {
			t.Errorf("finalisation must not be sent for a transfer that failed")
			w.WriteHeader(http.StatusOK)

			return
		}

		http.Error(w, "importer says no", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	log, _ := captureLog()
	require.Error(t, runUpload(t, srv, src, log, "-c", "1"))
}

// --- the --resume flag still means what it meant ---------------------------

// TestUpload_ResumeFlagContinuesFromPreviousRun holds the flag's own contract:
// with --resume the command asks the importer where a PREVIOUS run left off and
// starts there, sending only the remainder.
func TestUpload_ResumeFlagContinuesFromPreviousRun(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(10_000, 7)

	im := newImporter(t, int64(len(src)))
	im.preloaded = src[:6_000]

	srv := im.start()

	log, _ := captureLog()
	require.NoError(t, runUpload(t, srv, src, log, "-c", "1", "--resume"))

	requireSameBytes(t, src, im.snapshot())

	offsets := im.offsets()
	require.Len(t, offsets, 1, "the remainder is one chunk and needs one PUT")
	require.Equal(t, int64(6_000), offsets[0],
		"--resume must start from the offset the importer named, not from zero")
	require.Equal(t, 1, im.finishedCount())
}

// TestUpload_WithoutResumeFlagStartsFromZero is the other side of the same
// contract, and it is here so that the guard above cannot be satisfied by a
// command that consults the importer whether or not the flag was given.
func TestUpload_WithoutResumeFlagStartsFromZero(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(10_000, 8)

	im := newImporter(t, int64(len(src)))
	im.preloaded = src[:6_000]

	srv := im.start()

	log, _ := captureLog()
	err := runUpload(t, srv, src, log, "-c", "1")

	// Without the flag the command opens at zero; the importer, which holds a
	// prefix already, refuses and names where it stands, and the command
	// continues from there. What matters here is the FIRST offset it tried.
	require.NoError(t, err)

	offsets := im.offsets()
	require.NotEmpty(t, offsets)
	require.Equal(t, int64(0), offsets[0],
		"without --resume the command must not consult the importer for a starting offset")
}

// --- a break that leaves no answer behind ---------------------------------

// TestUpload_BreakWithoutAnAnswerResumesFromWhatTheImporterKept holds the half
// of the fix that is specific to writing: when a request dies before any answer
// comes back, the client has no local measure of how much of it landed, so it
// has to ASK the importer and continue from there.
//
// What it does NOT hold: the classification of the break. The error an aborted
// handler produces here is one the shared classifier already recognises, so this
// guard would stay green for a client that never asked and simply retried the
// whole chunk — except that it also pins the offsets, and a client that did not
// ask re-sends from zero. The classification claim proper lives where it is
// made, in internal/dataplane
// (TestRetrier_RetriesNotAcceptedWithoutAnyDelivery).
func TestUpload_BreakWithoutAnAnswerResumesFromWhatTheImporterKept(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(30_000, 9)

	im := newImporter(t, int64(len(src)))
	im.breakAfter[1] = 12_000

	srv := im.start()

	log, out := captureLog()
	require.NoError(t, runUpload(t, srv, src, log, "-c", "1"))

	requireSameBytes(t, src, im.snapshot())

	offsets := im.offsets()
	require.Len(t, offsets, 2, "the break must have cost exactly one extra PUT")
	require.Equal(t, int64(0), offsets[0])
	require.Equal(t, int64(12_000), offsets[1],
		"the retry must continue from what the importer kept, not from zero and not from the whole chunk")

	require.Positive(t, im.headCount(), "the client must have asked the importer how far it got")

	lines := retryLines(out.String())
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "resume_offset=12000")
}

// TestUpload_BreakAfterTheLastByteIsNotReportedAsARetry holds the branch that
// settles a break whose bytes all landed: the connection died after the final
// chunk was taken and before its acknowledgement came back.
//
// That transfer ARRIVED, and the assertion that separates a correct reading from
// a merely-workable one is the absence of a retry line. A client that ended the
// attempt with a failure would still finish — the next attempt finds nothing
// left to send and returns — so the upload succeeds either way and only the
// evidence differs. The evidence is not decoration here: the live acceptance
// matrix reads exactly these lines to decide whether a break happened and was
// survived, so a run that prints one when nothing was retried reports a break
// that never occurred.
func TestUpload_BreakAfterTheLastByteIsNotReportedAsARetry(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(10_000, 15)

	im := newImporter(t, int64(len(src)))
	// The final chunk is taken in full, and only then does the connection die.
	im.breakAfter[2] = len(src)

	srv := im.start()

	log, out := captureLog()
	require.NoError(t, runUpload(t, srv, src, log, "-c", "2"))

	requireSameBytes(t, src, im.snapshot())
	require.Equal(t, 1, im.finishedCount())
	require.Equal(t, []int64{0, 5_000}, im.offsets(), "no chunk may be sent twice")
	require.Empty(t, retryLines(out.String()),
		"a transfer whose last byte had already landed was not retried, and must not say it was")
}

// TestUpload_MissingDestinationIsNotComplete pins the rule that lets the size
// test above stand on its own: an importer with no such destination reports no
// length, and "no length" is a value no declared upload size can equal.
//
// The answer is given a length equal to the upload's own size precisely because
// that is the only way the rule can be seen working. A 404 carrying no length,
// or one carrying any other length, would be read correctly by a client that had
// no such rule at all.
func TestUpload_MissingDestinationIsNotComplete(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(6_000, 16)

	missing := int64(len(src))

	im := newImporter(t, int64(len(src)))
	im.headSize = &missing
	im.headStatus = http.StatusNotFound

	for put := 1; put <= im.maxPuts; put++ {
		im.breakAfter[put] = 400
	}

	srv := im.start()

	log, _ := captureLog()
	err := runUpload(t, srv, src, log, "-c", "1")

	require.Error(t, err, "a destination the importer does not have is not a finished upload")
	require.Zero(t, im.finishedCount(), "an import must not be finalised over a destination that is not there")
}

// TestUpload_UnresolvableConflictBuysAnotherAttempt holds the claim that a
// conflict the client cannot get an answer about is a reason to wait, not a
// verdict. The importer refuses without naming an offset and then fails the
// question too, which is what the moment after a break looks like while the
// previous request's handler is still draining.
//
// Both a correct client and one that treated the unanswerable conflict as fatal
// end this run with an error, so the error is not the discriminator — the number
// of attempts is. A verdict costs one request; waiting costs several and gives
// the draining handler the time it needs.
func TestUpload_UnresolvableConflictBuysAnotherAttempt(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(6_000, 17)

	im := newImporter(t, int64(len(src)))
	im.headStatus = http.StatusInternalServerError
	im.headSize = new(int64)

	for put := 1; put <= im.maxPuts; put++ {
		im.conflicts[put] = conflict{}
	}

	srv := im.start()

	log, _ := captureLog()
	err := runUpload(t, srv, src, log, "-c", "1")

	require.Error(t, err)
	require.Zero(t, im.finishedCount())
	require.Greater(t, len(im.offsets()), 1,
		"an importer that could not answer was treated as a verdict instead of as a reason to wait")
	require.LessOrEqual(t, len(im.offsets()), im.maxPuts)
}

// --- an importer that acknowledges and accepts nothing ---------------------

// TestUpload_AcknowledgementThatDoesNotMoveIsBounded holds a branch none of the
// guards above reaches: a 2xx that names back the offset it was sent at. Nothing
// is wrong with the connection, nothing is refused, and the destination simply
// never grows — so a loop that trusts a 2xx to mean forward motion spins here
// with no break to end it and no budget to spend.
//
// The stub bounds itself for that reason: an unbounded client fails this by
// tripping maxPuts rather than by hanging until the package timeout.
func TestUpload_AcknowledgementThatDoesNotMoveIsBounded(t *testing.T) {
	fastRetries(t)

	src := pseudoRandom(4_000, 11)

	im := newImporter(t, int64(len(src)))
	im.acknowledgeWithoutMoving = true

	srv := im.start()

	log, _ := captureLog()
	err := runUpload(t, srv, src, log, "-c", "1")

	require.Error(t, err, "an importer that never moves must end the command, not spin it")
	require.Zero(t, im.finishedCount(), "nothing arrived, so nothing may be finalised")
	require.LessOrEqual(t, len(im.offsets()), im.maxPuts)
}

// TestAcceptedOffset covers the reading of X-Next-Offset on an accepted chunk in
// isolation, including the two malformed answers a live importer should never
// send and which would otherwise move the durable offset to a number nobody
// agreed on.
func TestAcceptedOffset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		header  string
		offset  int64
		sendLen int64
		want    int64
		wantErr bool
	}{
		{name: "no header means the whole chunk landed", header: "", offset: 100, sendLen: 50, want: 150},
		{name: "a named offset wins over the arithmetic", header: "120", offset: 100, sendLen: 50, want: 120},
		{name: "an offset equal to the current one is returned as is", header: "100", offset: 100, sendLen: 50, want: 100},
		{name: "an offset below the current one is refused", header: "90", offset: 100, sendLen: 50, wantErr: true},
		{name: "a non-numeric offset is refused", header: "soon", offset: 100, sendLen: 50, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := acceptedOffset(tc.header, tc.offset, tc.sendLen)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// --- the stub's own contract ----------------------------------------------

// TestImporterStub_AnswersTheContractItClaims keeps the guards above honest. All
// of them lean on the stub answering a full destination with a size and no
// resume offset, and a partial one with an offset — and a stub that answered
// otherwise would make several of them pass for reasons of its own.
func TestImporterStub_AnswersTheContractItClaims(t *testing.T) {
	src := pseudoRandom(1_000, 10)

	im := newImporter(t, int64(len(src)))
	srv := im.start()

	head := func() *http.Response {
		t.Helper()

		req, err := http.NewRequestWithContext(context.TODO(), http.MethodHead, srv.URL+dataSubpath+dstPath, nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })

		return resp
	}

	require.Equal(t, http.StatusNotFound, head().StatusCode, "an untouched destination must not exist")

	im.mu.Lock()
	im.loaded = true
	im.accepted = append(im.accepted, src[:400]...)
	im.mu.Unlock()

	partial := head()
	require.Equal(t, http.StatusOK, partial.StatusCode)
	require.Equal(t, "400", partial.Header.Get("X-Next-Offset"), "a partial destination must name its resume offset")

	im.mu.Lock()
	im.accepted = append(im.accepted, src[400:]...)
	im.mu.Unlock()

	full := head()
	require.Equal(t, http.StatusOK, full.StatusCode)
	require.Empty(t, full.Header.Get("X-Next-Offset"), "a full destination must name NO resume offset")
	require.Equal(t, int64(len(src)), full.ContentLength, "a full destination must report its size")
}
