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

package exporter

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
)

// ErrExpired is returned by WaitReady when the DataExport enters the Expired
// terminal state and can no longer be used for data transfer.
var ErrExpired = errors.New("DataExport expired")

// ErrTargetRefMismatch is returned by EnsureDataExport when a same-named
// DataExport CR already exists but its Spec.TargetRef names a DIFFERENT object
// than the request. This can occur after a hash collision or when a DataExport
// was created manually under the deterministic name. Reusing its endpoint would
// download the wrong object's bytes. EnsureDataExport refuses instead of
// silently reusing or deleting it.
var ErrTargetRefMismatch = errors.New("existing DataExport targets a different object")

// ErrTargetUIDMismatch is returned by EnsureDataExport when an observed
// DataExport is not stamped for the exact Snapshot CR UID requested by the
// caller. The targetRef contract does not carry UID, so the client-owned
// annotation is the authoritative lifecycle discriminator.
var ErrTargetUIDMismatch = errors.New("existing DataExport targets a different object UID")

// ErrTargetUIDRequired is returned when a caller attempts to open a DataExport
// lifecycle without the exact Snapshot CR UID.
var ErrTargetUIDRequired = errors.New("snapshot target UID is required")

// ErrPublishForeignOwner is returned when --publish requires enabling
// spec.publish on a DataExport that ANOTHER live download run owns. Adoption of a
// foreign CR is read-only by contract, so this run refuses to mutate it and fails
// fast instead of waiting out the full readiness timeout for a status.publicURL
// that will never appear.
var ErrPublishForeignOwner = errors.New("DataExport owned by another download run has publish disabled")

// errDataExportRecheck signals that the adopted DataExport changed under our feet
// while alignDataExportPublish was aligning spec.publish — it vanished, started
// terminating, or expired. EnsureDataExport falls through to its create path
// instead of returning the object it can no longer vouch for.
var errDataExportRecheck = errors.New("DataExport changed during publish alignment")

// defaultDataExportTTL is the fallback TTL used for DataExport when the caller
// passes an empty string. Snapshot transfers can be large, so we use a longer
// default than the 2-minute interactive default.
const defaultDataExportTTL = "2h"

// logEveryN is the poll-attempt cadence at which WaitReady emits a progress log.
// With a 3 s poll interval, every 5 attempts ≈ 15 s.
const logEveryN = 5

// The DataExport terminal Expired state is signalled by the producer as Ready=False with reason
// "Expired" (the standalone "Expired" condition type was removed from the catalog in favour of a reason
// plus a status.phase). After a retention TTL the DataExport garbage collector then deletes the CR, so
// EnsureDataExport and WaitReady recognize this Ready reason via dataExportExpired.
const (
	conditionTypeReady = "Ready"
	reasonExpired      = "Expired"
)

// dataExportExpired reports whether the DataExport has terminally idle-expired (Ready=False/Expired).
func dataExportExpired(conds []metav1.Condition) bool {
	c := meta.FindStatusCondition(conds, conditionTypeReady)
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == reasonExpired
}

// exportBaseURL returns the base URL this run must talk to: the
// storage-foundation-published Ingress URL when the public endpoint was
// requested, otherwise the in-cluster exporter URL. The public URL carries a
// path prefix (https://<host>/<namespace>/<kind-short>/<name>/) while the
// internal one does not, so callers must join request paths onto THIS value and
// never re-derive them from the origin alone.
func exportBaseURL(de *deapi.DataExport, publicEndpoint bool) string {
	if publicEndpoint {
		return de.Status.PublicURL
	}

	return de.Status.URL
}

// dataExportGonePollInterval is the poll cadence EnsureDataExport uses while waiting
// for a terminating DataExport (DeletionTimestamp set) to fully vanish before it
// recreates a fresh one. It is short because the controller's finalizer unwinding
// completes in seconds on a real cluster. The wait is bounded by ctx and, when the
// caller passes WithTerminatingWaitTimeout, by an explicit cap on top of ctx.
const dataExportGonePollInterval = 500 * time.Millisecond

// dataExportGoneLogEveryN is the poll-attempt cadence at which waitForDataExportGone
// emits a progress log. With a 500 ms poll interval, every 30 attempts ≈ 15 s, matching
// WaitReady's ≈15 s cadence so a slow finalizer unwind is observable instead of a silent
// spinner.
const dataExportGoneLogEveryN = 30

// runOwnerAnnotation records the download run that CREATED (and therefore owns) a
// DataExport CR. The CR name is deterministic, so two
// concurrent download runs targeting the same leaf resolve to the SAME CR; this
// annotation lets each run tell "the CR I created" from "a CR another live run
// created" so a run never deletes or hijacks another run's in-flight export
// (inv #10b). The value is an opaque per-run hex ID (pipeline.Config.RunID).
const runOwnerAnnotation = "snapshot.deckhouse.io/download-run-id"

// targetUIDAnnotation binds a DataExport to the exact Snapshot CR incarnation.
// DataExport targetRef has no UID field, so GVK/name alone cannot distinguish a
// deleted and recreated Snapshot.
const targetUIDAnnotation = "snapshot.deckhouse.io/target-uid"

// DataExportName derives a deterministic DataExport CR name from the canonical
// namespaced target identity, including the exact Snapshot CR UID. The readable
// leaf prefix is normalized only for display; the hash covers the original,
// unnormalized identity so identities that normalize alike remain distinct.
// The result is a DNS-1123 label no longer than 63 bytes, which also satisfies
// Kubernetes object-name limits.
func DataExportName(namespace, group, resource, kind, leafName string, targetUID types.UID) string {
	const (
		prefix       = "de-"
		hashLength   = 20
		maxNameBytes = 63
	)

	identity := strings.Join([]string{namespace, group, resource, kind, leafName, string(targetUID)}, "\x00")
	sum := sha256.Sum256([]byte(identity))
	hash := fmt.Sprintf("%x", sum[:])[:hashLength]

	readable := normalizeDNSLabel(leafName)
	maxReadable := maxNameBytes - len(prefix) - 1 - len(hash)

	if len(readable) > maxReadable {
		readable = strings.Trim(readable[:maxReadable], "-")
	}

	if readable == "" {
		return prefix + hash
	}

	return prefix + readable + "-" + hash
}

func normalizeDNSLabel(value string) string {
	var normalized strings.Builder

	lastDash := false

	for _, char := range strings.ToLower(value) {
		valid := char >= 'a' && char <= 'z' || char >= '0' && char <= '9'
		if valid {
			normalized.WriteRune(char)

			lastDash = false

			continue
		}

		if normalized.Len() > 0 && !lastDash {
			normalized.WriteByte('-')

			lastDash = true
		}
	}

	return strings.Trim(normalized.String(), "-")
}

// targetRefMatches reports whether an existing DataExport's targetRef refers to
// the same object as the requested {group, resource, kind, name}. Group and Name
// are compared strictly (Group is never pruned and is "" for the core group; Name
// is always populated). Resource and Kind are compared only when populated on BOTH
// sides: the deployed SVDM CRD prunes whichever of resource/kind it does not
// understand (see TargetRefSpec's TEMP REVERTME note), so a re-fetched CR
// legitimately carries an empty resource OR kind for a MATCHING target — treating
// that as a mismatch would wrongly reject the happy-path adoption on a real
// cluster. A populated field that DIFFERS is a true mismatch (a distinct object
// aliasing the same de-<leaf> name).
func targetRefMatches(existing deapi.TargetRefSpec, group, resource, kind, name string) bool {
	if existing.Name != name || existing.Group != group {
		return false
	}

	if existing.Resource != "" && resource != "" && existing.Resource != resource {
		return false
	}

	if existing.Kind != "" && kind != "" && existing.Kind != kind {
		return false
	}

	return true
}

// targetRefMismatchError builds the wrapped ErrTargetRefMismatch returned whenever
// an existing/observed DataExport named deName targets a different object than the
// request. It names BOTH the observed targetRef (got) and the request so the
// operator can resolve the collision. Shared by the live-CR adoption path and the
// post-Create re-fetch path so both report the identical, actionable message.
func targetRefMismatchError(deName string, got deapi.TargetRefSpec, group, resource, kind, name string) error {
	return fmt.Errorf(
		"%w: DataExport %q already targets {group=%q resource=%q kind=%q name=%q}, "+
			"but this request is for {group=%q resource=%q kind=%q name=%q}; "+
			"delete the stale DataExport or resolve the name collision before retrying",
		ErrTargetRefMismatch, deName,
		got.Group, got.Resource, got.Kind, got.Name,
		group, resource, kind, name,
	)
}

func targetUIDMismatchError(deName string, got string, want types.UID) error {
	return fmt.Errorf(
		"%w: DataExport %q has target UID annotation %q, but this request is for UID %q; "+
			"leave the existing DataExport untouched and retry with its original Snapshot or remove it explicitly",
		ErrTargetUIDMismatch, deName, got, want,
	)
}

// ensureOptions carries optional per-run ownership context for EnsureDataExport.
type ensureOptions struct {
	runID              string
	targetUID          types.UID
	log                *slog.Logger
	terminatingTimeout time.Duration
	acquisition        **DataExportAcquisition
	publish            bool
}

// EnsureOption configures optional behavior of EnsureDataExport.
type EnsureOption func(*ensureOptions)

// DataExportAcquisition is operation-scoped evidence that EnsureDataExport
// successfully acquired one exact DataExport object. Its fields are private so
// cleanup callers must obtain it from WithAcquisition rather than infer deletion
// authority from a deterministic name or run annotation.
type DataExportAcquisition struct {
	namespace          string
	name               string
	uid                types.UID
	targetRef          deapi.TargetRefSpec
	targetUID          types.UID
	runID              string
	ownerAtAcquisition string
}

// Name returns the acquired DataExport name.
func (a *DataExportAcquisition) Name() string {
	if a == nil {
		return ""
	}

	return a.name
}

// UID returns the exact UID observed when the DataExport was acquired.
func (a *DataExportAcquisition) UID() types.UID {
	if a == nil {
		return ""
	}

	return a.uid
}

// TargetRef returns the exact targetRef observed when the DataExport was acquired.
func (a *DataExportAcquisition) TargetRef() deapi.TargetRefSpec {
	if a == nil {
		return deapi.TargetRefSpec{}
	}

	return a.targetRef
}

// TargetUID returns the exact Snapshot CR UID bound to the acquisition.
func (a *DataExportAcquisition) TargetUID() types.UID {
	if a == nil {
		return ""
	}

	return a.targetUID
}

// WithAcquisition records operation-scoped cleanup evidence when
// EnsureDataExport successfully returns an exact DataExport. The output remains
// nil on every pre-acquisition failure, including a targetRef mismatch.
func WithAcquisition(out **DataExportAcquisition) EnsureOption {
	return func(o *ensureOptions) {
		o.acquisition = out
	}
}

// WithTargetUID binds the DataExport lifecycle to the exact Snapshot CR
// incarnation. EnsureDataExport rejects calls that omit it.
func WithTargetUID(targetUID types.UID) EnsureOption {
	return func(o *ensureOptions) {
		o.targetUID = targetUID
	}
}

// WithTerminatingWaitTimeout bounds the wait EnsureDataExport performs when it
// observes the DataExport already TERMINATING (DeletionTimestamp set) and must
// wait for it to fully vanish before recreating a fresh one. Without this cap the
// wait is bounded only by ctx, so a caller that passes a deadline-less ctx (e.g.
// the raw download-run ctx used by the pipeline's stamp-Ensure) would hang the
// whole run FOREVER on a wedged finalizer or a downed DataExport controller, with
// no output — a ctx that merely CAN carry a deadline is not enough (code-style §6).
// The pipeline derives d from the run's ReadinessTimeout. A non-positive d leaves
// the wait bounded by ctx alone (the pre-existing behavior for callers that do not
// opt in).
func WithTerminatingWaitTimeout(d time.Duration) EnsureOption {
	return func(o *ensureOptions) {
		o.terminatingTimeout = d
	}
}

// WithRunOwner makes EnsureDataExport stamp runID as the owning run
// (runOwnerAnnotation) on any DataExport it CREATES, and log an explicit WARN via
// log when it instead adopts a live CR that a DIFFERENT run already owns. The
// adopted endpoint is still reused for read-only transfer, but ownership — and
// therefore the right to delete the CR on release (ReleaseDataExport) — stays
// with the other run, so neither run tears down the other's in-flight export
// (inv #10b). runID must be non-empty to take effect; a nil log disables the
// adoption WARN.
func WithRunOwner(runID string, log *slog.Logger) EnsureOption {
	return func(o *ensureOptions) {
		o.runID = runID
		o.log = log
	}
}

// WithPublish makes EnsureDataExport request the storage-foundation-published
// (Ingress) endpoint for this DataExport: spec.publish is set on any CR this call
// CREATES, and an adopted CR that still has spec.publish=false is upgraded in place
// (see alignDataExportPublish). Publish is never downgraded back to false.
func WithPublish(publish bool) EnsureOption {
	return func(o *ensureOptions) {
		o.publish = publish
	}
}

func (o ensureOptions) recordAcquisition(de *deapi.DataExport) error {
	if o.acquisition == nil {
		return nil
	}

	if de.UID == "" {
		return fmt.Errorf("acquire DataExport %q: API server returned an empty UID", de.Name)
	}

	*o.acquisition = &DataExportAcquisition{
		namespace:          de.Namespace,
		name:               de.Name,
		uid:                de.UID,
		targetRef:          de.Spec.TargetRef,
		targetUID:          o.targetUID,
		runID:              o.runID,
		ownerAtAcquisition: de.Annotations[runOwnerAnnotation],
	}

	return nil
}

// warnIfForeign logs a WARN when this run is adopting a live DataExport that a
// DIFFERENT run already owns. An unstamped CR (no owner annotation) is treated as
// unowned and adopted silently, preserving pre-ownership behavior.
func (o ensureOptions) warnIfForeign(de *deapi.DataExport, deName string) {
	if o.runID == "" || o.log == nil {
		return
	}

	owner := de.Annotations[runOwnerAnnotation]
	if owner == "" || owner == o.runID {
		return
	}

	o.log.Warn("adopting DataExport owned by another download run; will not release it",
		slog.String("name", deName),
		slog.String("owner", owner),
		slog.String("run_id", o.runID))
}

// alignDataExportPublish upgrades an ADOPTED DataExport's spec.publish from false
// to true so this run gets a status.publicURL. It never writes false: downgrading
// makes the storage-foundation controller DELETE the public Service and Ingress
// (reconcilePublishResources), which would tear the endpoint out from under a
// concurrent run still streaming through it. Returns the up-to-date object.
//
// The patch carries an OPTIMISTIC LOCK. spec.publish is the first spec field this
// client writes on a CR it did not necessarily create, and the deterministic CR
// name means two concurrent download runs resolve to the SAME object; the
// storage-foundation controller also writes conditions, status.url and finalizers
// on it continuously. The lock guarantees the write lands on the EXACT revision
// whose identity was validated in this same attempt.
//
// A conflict is never re-sent from a stale base: retry.RetryOnConflict drops the
// cached object, re-Gets it and re-runs EVERY identity check before patching
// again. Once retry.DefaultRetry is exhausted the conflict is returned to the
// caller, which fails the leaf so the operator's next resume run recomputes the
// target from scratch.
//
// This deliberately diverges from the older, already-shipped one-way publish
// upgrade in internal/data/dataexport/util/util.go (EnsureDataExportPublish,
// used by "d8 data export download"): that helper patches with plain
// client.MergeFrom and no identity re-validation. That path predates the
// deterministic-name, multi-run-adoption model this package documents (see
// runOwnerAnnotation), so a stale write there is far less likely to collide
// with a concurrent, identity-distinct owner. Here it is not — hence the lock
// and the re-validation. EnsureDataExportPublish is intentionally left as-is;
// this function does not replace or call it.
func (o ensureOptions) alignDataExportPublish(
	ctx context.Context,
	c client.Client,
	existing *deapi.DataExport,
	group, resource, kind, leafName string,
) (*deapi.DataExport, error) {
	current := existing
	didPatch := false
	deName := existing.Name

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if current == nil {
			latest := new(deapi.DataExport)

			getErr := c.Get(ctx, client.ObjectKey{Namespace: existing.Namespace, Name: deName}, latest)
			if kubeerrors.IsNotFound(getErr) {
				return errDataExportRecheck
			}

			if getErr != nil {
				return fmt.Errorf("get DataExport %q after conflict: %w", deName, getErr)
			}

			current = latest
		}

		if !targetRefMatches(current.Spec.TargetRef, group, resource, kind, leafName) {
			return targetRefMismatchError(deName, current.Spec.TargetRef, group, resource, kind, leafName)
		}

		if current.Annotations[targetUIDAnnotation] != string(o.targetUID) {
			return targetUIDMismatchError(deName, current.Annotations[targetUIDAnnotation], o.targetUID)
		}

		if current.DeletionTimestamp != nil || dataExportExpired(current.Status.Conditions) {
			return errDataExportRecheck
		}

		if current.Spec.Publish {
			return nil
		}

		owner := current.Annotations[runOwnerAnnotation]
		if o.runID != "" && owner != "" && owner != o.runID {
			return fmt.Errorf(
				"%w: DataExport %q is owned by download run %q and has spec.publish=false; "+
					"wait for that run to finish, or rerun without --publish to use the in-cluster endpoint",
				ErrPublishForeignOwner, deName, owner)
		}

		base := current.DeepCopy()
		current.Spec.Publish = true

		if patchErr := c.Patch(ctx, current,
			client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); patchErr != nil {
			current = nil
			return patchErr
		}

		didPatch = true

		return nil
	})
	if err != nil {
		if errors.Is(err, errDataExportRecheck) ||
			errors.Is(err, ErrPublishForeignOwner) ||
			errors.Is(err, ErrTargetRefMismatch) ||
			errors.Is(err, ErrTargetUIDMismatch) {
			return nil, err
		}

		return nil, fmt.Errorf("patch DataExport %q spec.publish: %w", deName, err)
	}

	if didPatch && o.log != nil {
		o.log.Info("enabled publish on adopted DataExport",
			slog.String("name", deName),
			slog.String("run_id", o.runID))
	}

	return current, nil
}

// lifecycleAnnotations always stamps the target Snapshot UID and additionally
// stamps runID when the caller tracks per-run ownership.
func lifecycleAnnotations(runID string, targetUID types.UID) map[string]string {
	annotations := map[string]string{targetUIDAnnotation: string(targetUID)}
	if runID != "" {
		annotations[runOwnerAnnotation] = runID
	}

	return annotations
}

// EnsureDataExport idempotently creates a DataExport in namespace targeting
// the snapshot leaf CR identified by {group, kind, leafName, target UID} with
// the given TTL (empty → "2h"). Returns the DataExport object (newly created
// or pre-existing).
//
// group and kind must identify a namespaced snapshot CR (e.g.
// "snapshot.storage.k8s.io" / "VolumeSnapshot" for a CSI VolumeSnapshot leaf, or
// the domain group / kind for a domain snapshot CR). The controller routes any
// such targetRef through its kind-agnostic categorySnapshot path.
//
// Pass WithRunOwner to scope ownership to a single download run: the run stamps
// its ID on any CR it creates and is warned when it adopts a CR another live run
// owns (see WithRunOwner and inv #10b). Without it, EnsureDataExport keeps its
// original ownership-agnostic behavior.
func EnsureDataExport(
	ctx context.Context,
	c client.Client,
	namespace,
	group,
	resource,
	kind,
	leafName,
	ttl string,
	opts ...EnsureOption,
) (*deapi.DataExport, error) {
	var o ensureOptions

	for _, opt := range opts {
		opt(&o)
	}

	if o.acquisition != nil {
		*o.acquisition = nil
	}

	if o.targetUID == "" {
		return nil, fmt.Errorf("%w for %s/%s", ErrTargetUIDRequired, namespace, leafName)
	}

	deName := DataExportName(namespace, group, resource, kind, leafName, o.targetUID)

	existing := new(deapi.DataExport)

	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, existing)
	if err == nil {
		// Validate identity before any lifecycle action. In particular, an
		// expired or terminating collision is foreign to this operation and must
		// not be deleted, waited out, or otherwise mutated to make progress.
		if !targetRefMatches(existing.Spec.TargetRef, group, resource, kind, leafName) {
			return nil, targetRefMismatchError(deName, existing.Spec.TargetRef, group, resource, kind, leafName)
		}

		if existing.Annotations[targetUIDAnnotation] != string(o.targetUID) {
			return nil, targetUIDMismatchError(deName, existing.Annotations[targetUIDAnnotation], o.targetUID)
		}

		switch {
		case existing.DeletionTimestamp != nil:
			// The CR is TERMINATING: an interrupted run's release defer (or the
			// Expired reclaim below) already deleted it, and the controller is
			// still unwinding the export chain. Adopting it would be fatal — its
			// endpoint is doomed and WaitReady's first Get races the finalizer
			// removal into NotFound, failing the whole run in the interrupt→resume
			// workflow this feature exists for. Do NOT adopt: wait (ctx-bounded)
			// for it to vanish, then fall through to the create path so this run
			// gets a fresh, this-run-owned CR. Mirrors how the Expired reclaim
			// already tolerates delete propagation.
			//
			// The wait is bounded by waitCtx: WithTerminatingWaitTimeout caps it
			// (on top of ctx) so a wedged finalizer or a downed controller cannot
			// hang the run forever even under a deadline-less ctx (code-style §6).
			waitCtx, waitCancel := terminatingWaitContext(ctx, o.terminatingTimeout)

			waitErr := waitForDataExportGone(waitCtx, c, o.log, namespace, deName)

			waitCancel()

			if waitErr != nil {
				return nil, waitErr
			}

		case !dataExportExpired(existing.Status.Conditions):
			// If a different run owns it, this run is adopting a foreign export
			// read-only and must not release it (warnIfForeign logs the adoption).
			// Ownership is intentionally NOT changed on adoption.
			o.warnIfForeign(existing, deName)

			if o.publish {
				aligned, alignErr := o.alignDataExportPublish(ctx, c, existing, group, resource, kind, leafName)
				if alignErr != nil {
					if !errors.Is(alignErr, errDataExportRecheck) {
						return nil, alignErr
					}

					// The object started terminating mid-retry (observed inside
					// alignDataExportPublish, not here). Falling through to Create
					// below without an explicit waitForDataExportGone is deliberate,
					// not an oversight: Create already swallows AlreadyExists and every
					// fallthrough path re-fetches and re-validates identity (see the
					// comment above the Create call), so at worst this run adopts the
					// still-terminating object for one pass and converges to a fresh,
					// this-run-owned CR on the next resume attempt — the same
					// "one-run delay, not a regression" the Expired branch below
					// already accepts. Adding a wait here would just duplicate that
					// case's handling for a narrower trigger.
					break
				}

				existing = aligned
			}

			if err := o.recordAcquisition(existing); err != nil {
				return nil, err
			}

			return existing, nil

		default:
			// The producer's GC only deletes an expired DataExport after its retention TTL (~24h), so
			// within that window a stale expired object (Ready=False/Expired) from a previous session
			// would otherwise be returned forever, permanently blocking resume. Delete it and fall through
			// to the normal create path below. This reclaim is deliberately
			// OWNER-AGNOSTIC (a crashed owner's CR is reclaimed via TTL exactly as before);
			// the recreated CR below is stamped with THIS run's ownership.
			// Delete is not synchronous on a real cluster: the object may still be
			// terminating when the Create below runs, which can race into
			// AlreadyExists (swallowed) and hand the caller back the same stale
			// Expired object on this pass. That is a one-run delay, not a
			// regression — the caller's per-node retry on the next resume attempt
			// (pipeline.Run is best-effort per node) converges once the delete has
			// actually propagated.
			observedUID := existing.UID
			if delErr := c.Delete(ctx, existing, client.Preconditions{UID: &observedUID}); delErr != nil &&
				!kubeerrors.IsNotFound(delErr) {
				return nil, fmt.Errorf("delete expired DataExport %q: %w", deName, delErr)
			}
		}
	} else if !kubeerrors.IsNotFound(err) {
		return nil, fmt.Errorf("get DataExport %q: %w", deName, err)
	}

	if ttl == "" {
		ttl = defaultDataExportTTL
	}

	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deName,
			Namespace:   namespace,
			Annotations: lifecycleAnnotations(o.runID, o.targetUID),
		},
		Spec: deapi.DataexportSpec{
			TTL: ttl,
			// Always set explicitly, including false: alignDataExportPublish compares this
			// field against the server's returned value, and the CRD declares no default:
			// for spec.publish, so an implicit value would make that comparison depend on
			// something we never sent.
			Publish: o.publish,
			TargetRef: deapi.TargetRefSpec{
				Group:    group,
				Resource: resource,
				Kind:     kind,
				Name:     leafName,
			},
		},
	}

	createErr := c.Create(ctx, de)
	created := createErr == nil

	switch {
	case created:
		// Create returns the API-server-populated object. Retain its exact UID
		// before any subsequent call can fail or observe cancellation.
		if err := o.recordAcquisition(de); err != nil {
			return nil, err
		}
	case kubeerrors.IsAlreadyExists(createErr):
		// This operation did not create the object. Acquisition remains nil
		// until the re-fetched object passes every identity check below.
	default:
		return nil, fmt.Errorf("create DataExport %q: %w", deName, createErr)
	}

	// Re-fetch so the returned object carries the server-assigned resource version.
	fetched := new(deapi.DataExport)

	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, fetched); err != nil {
		return nil, fmt.Errorf("get DataExport %q after create: %w", deName, err)
	}

	// The Create above deliberately swallows AlreadyExists, and every branch that
	// falls through to it (first-Get NotFound, the Expired reclaim delete, the
	// terminating-CR wait) may observe a CR this run did not create: a concurrent
	// actor can win the Get→Create window and leave a de-<leaf> CR targeting a
	// DIFFERENT object (the same name-aliasing the first-Get guard above prevents).
	// Returning it unchecked would stream the wrong object's bytes and checksum them
	// as complete forever. Re-run the identical targetRef check here so the guard
	// covers the create/re-fetch path too; our own just-created (matching) CR — the
	// overwhelmingly common case — passes unchanged.
	if !targetRefMatches(fetched.Spec.TargetRef, group, resource, kind, leafName) {
		return nil, targetRefMismatchError(deName, fetched.Spec.TargetRef, group, resource, kind, leafName)
	}

	if fetched.Annotations[targetUIDAnnotation] != string(o.targetUID) {
		return nil, targetUIDMismatchError(deName, fetched.Annotations[targetUIDAnnotation], o.targetUID)
	}

	if created {
		if fetched.UID != de.UID {
			return nil, fmt.Errorf(
				"DataExport %q UID changed after create: created %q, fetched %q",
				deName, de.UID, fetched.UID,
			)
		}
	} else if err := o.recordAcquisition(fetched); err != nil {
		return nil, err
	}

	return fetched, nil
}

// terminatingWaitContext derives the context that bounds the terminating-DataExport
// wait. When timeout > 0 it caps the wait at timeout ON TOP OF ctx, so a wedged
// finalizer or a downed controller cannot hang the run even under a deadline-less
// parent ctx (code-style §6); otherwise it returns ctx with a no-op cancel so the
// wait keeps its ctx-only bound (legacy callers that do not opt in).
func terminatingWaitContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}

	return ctx, func() {}
}

// waitForDataExportGone polls until the DataExport named deName in namespace is
// reported NotFound (its finalizers have fully unwound and it is gone) or ctx is
// done. It exists so EnsureDataExport can wait out a CR observed in the terminating
// state instead of adopting a doomed object. On ctx cancellation (deadline or
// SIGINT) it returns a wrapped ctx.Err() promptly, naming the CR and a kubectl
// inspection hint so a stuck unwind is diagnosable rather than a silent hang. A
// periodic Info line is emitted via log (nil disables it). No time.Sleep: the poll
// blocks in a select on ctx.
func waitForDataExportGone(ctx context.Context, c client.Client, log *slog.Logger, namespace, deName string) error {
	key := client.ObjectKey{Namespace: namespace, Name: deName}

	for attempt := 0; ; attempt++ {
		probe := new(deapi.DataExport)

		err := c.Get(ctx, key, probe)
		if kubeerrors.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("get terminating DataExport %q: %w", deName, err)
		}

		logTerminatingWait(ctx, log, namespace, deName, attempt)

		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"wait for terminating DataExport %q to be deleted: %w\n\n"+
					"To inspect DataExport status, run:\n  d8 k -n %s get dataexport %s -o yaml",
				deName, ctx.Err(), namespace, deName,
			)
		case <-time.After(dataExportGonePollInterval):
		}
	}
}

// logTerminatingWait emits a periodic Info line while waitForDataExportGone polls,
// so a slow finalizer unwind (or a downed controller) is observable instead of a
// silent spinner. It mirrors WaitReady's first-and-every-N cadence and carries a
// kubectl inspection hint naming the terminating CR. A nil log disables it.
func logTerminatingWait(ctx context.Context, log *slog.Logger, namespace, deName string, attempt int) {
	if log == nil {
		return
	}

	if attempt != 0 && attempt%dataExportGoneLogEveryN != 0 {
		return
	}

	attrs := make([]slog.Attr, 0, 5)
	attrs = append(attrs,
		slog.String("namespace", namespace),
		slog.String("name", deName),
		slog.Int("attempt", attempt),
		slog.String("inspect_hint", fmt.Sprintf("d8 k -n %s get dataexport %s -o yaml", namespace, deName)),
	)

	if deadline, ok := ctx.Deadline(); ok {
		attrs = append(attrs, slog.String("timeout_in", time.Until(deadline).Round(time.Second).String()))
	}

	log.LogAttrs(ctx, slog.LevelInfo, "waiting for terminating DataExport to be deleted", attrs...)
}

// readyConditionStatus returns a short status string from the DataExport conditions.
// It returns "reason: message" from the Ready condition, or "URL not assigned yet"
// when no Ready condition is present and the URL is still empty.
func readyConditionStatus(conds []metav1.Condition, hasURL bool) string {
	for _, cond := range conds {
		if cond.Type == "Ready" {
			msg := cond.Reason
			if cond.Message != "" {
				msg += ": " + cond.Message
			}

			return msg
		}
	}

	if !hasURL {
		return "URL not assigned yet"
	}

	return "waiting"
}

// waitReadyOptions carries optional readiness criteria for WaitReady.
type waitReadyOptions struct {
	publicEndpoint bool
}

// WaitReadyOption customizes what WaitReady treats as ready.
type WaitReadyOption func(*waitReadyOptions)

// WithPublicEndpoint makes WaitReady additionally require a non-empty
// status.publicURL. The controller can flip Ready=True BEFORE it has finished
// creating the public Service/Ingress and written status.publicURL, so returning
// on Ready alone would hand the caller an empty base URL.
//
// Named after the endpoint, not "published": in this codebase "published" already
// means an atomically committed on-disk artifact (archive.PublicationPublished).
func WithPublicEndpoint() WaitReadyOption {
	return func(o *waitReadyOptions) {
		o.publicEndpoint = true
	}
}

// WaitReady polls the DataExport named deName until:
//   - its Ready condition is True and its base URL (status.url, or
//     status.publicURL when WithPublicEndpoint is passed) is populated → returns the DE,
//   - it is Ready=False with reason Expired → returns a wrapped ErrExpired,
//   - ctx is cancelled or its deadline is exceeded → returns a wrapped ctx.Err()
//     that includes the last observed DataExport status and an inspection hint.
//
// The poll interval is 3 s. A log line is emitted on the first poll and every
// logEveryN polls (≈15 s) to avoid spamming output while the export initialises.
// Callers set a deadline via ctx to bound the wait.
func WaitReady(
	ctx context.Context,
	c client.Client,
	log *slog.Logger,
	namespace,
	deName string,
	opts ...WaitReadyOption,
) (*deapi.DataExport, error) {
	var o waitReadyOptions

	for _, opt := range opts {
		opt(&o)
	}

	var lastStatus string

	var lastPublicURL string

	for attempt := 0; ; attempt++ {
		de := new(deapi.DataExport)

		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, de); err != nil {
			return nil, fmt.Errorf("get DataExport %q: %w", deName, err)
		}

		if dataExportExpired(de.Status.Conditions) {
			return nil, fmt.Errorf("DataExport %s/%s: %w", namespace, deName, ErrExpired)
		}

		lastPublicURL = de.Status.PublicURL

		hasURL := exportBaseURL(de, o.publicEndpoint) != ""
		if hasURL {
			for _, cond := range de.Status.Conditions {
				if cond.Type == "Ready" && cond.Status == metav1.ConditionTrue {
					return de, nil
				}
			}
		}

		lastStatus = readyConditionStatus(de.Status.Conditions, hasURL)

		if attempt == 0 || attempt%logEveryN == 0 {
			attrs := make([]slog.Attr, 0, 5)
			attrs = append(attrs,
				slog.String("namespace", namespace),
				slog.String("name", deName),
				slog.String("status", lastStatus),
				slog.Int("attempt", attempt),
			)

			if deadline, ok := ctx.Deadline(); ok {
				attrs = append(attrs, slog.String("timeout_in", time.Until(deadline).Round(time.Second).String()))
			}

			log.LogAttrs(ctx, slog.LevelInfo, "waiting for DataExport to be ready", attrs...)
		}

		select {
		case <-ctx.Done():
			hint := ""
			if o.publicEndpoint && lastPublicURL == "" {
				hint = "\n\nspec.publish is set but status.publicURL is still empty: the storage-foundation " +
					"controller has not finished creating the public Service/Ingress. Check the module's " +
					"ingress configuration, or rerun without --publish to use the in-cluster endpoint."
			}

			return nil, fmt.Errorf(
				"%w; DataExport status: %s\n\nTo inspect DataExport status, run:\n  d8 k -n %s get dataexport %s -o yaml%s",
				ctx.Err(), lastStatus, namespace, deName, hint,
			)
		case <-time.After(3 * time.Second):
		}
	}
}

// ReleaseDataExport deletes only the exact DataExport represented by acquisition.
// Deterministic name or matching run annotation alone never grants authority.
// UID, targetRef, target UID annotation, and observed owner must all still
// match; a foreign owner is never deleted. UID-precondition conflicts and
// NotFound are idempotent success.
func ReleaseDataExport(
	ctx context.Context,
	c client.Client,
	log *slog.Logger,
	acquisition *DataExportAcquisition,
) error {
	if acquisition == nil || acquisition.uid == "" {
		return errors.New("release DataExport: valid acquisition evidence is required")
	}

	de := new(deapi.DataExport)

	err := c.Get(ctx, client.ObjectKey{Namespace: acquisition.namespace, Name: acquisition.name}, de)
	if kubeerrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("get DataExport %q before delete: %w", acquisition.name, err)
	}

	owner := de.Annotations[runOwnerAnnotation]

	targetUID := types.UID(de.Annotations[targetUIDAnnotation])
	if de.UID != acquisition.uid ||
		de.Spec.TargetRef != acquisition.targetRef ||
		targetUID != acquisition.targetUID ||
		owner != acquisition.ownerAtAcquisition {
		if log != nil {
			log.Warn("skipping DataExport release because acquired identity changed",
				slog.String("name", acquisition.name),
				slog.String("acquired_uid", string(acquisition.uid)),
				slog.String("current_uid", string(de.UID)),
				slog.String("acquired_target_uid", string(acquisition.targetUID)),
				slog.String("target_uid", string(targetUID)),
				slog.String("acquired_owner", acquisition.ownerAtAcquisition),
				slog.String("owner", owner),
				slog.String("run_id", acquisition.runID))
		}

		return nil
	}

	if owner != "" && owner != acquisition.runID {
		if log != nil {
			log.Warn("skipping DataExport release owned by another download run",
				slog.String("name", acquisition.name),
				slog.String("owner", owner),
				slog.String("run_id", acquisition.runID))
		}

		return nil
	}

	if delErr := c.Delete(ctx, de, client.Preconditions{UID: &acquisition.uid}); delErr != nil &&
		!kubeerrors.IsNotFound(delErr) && !kubeerrors.IsConflict(delErr) {
		return fmt.Errorf("delete DataExport %q: %w", acquisition.name, delErr)
	}

	return nil
}
