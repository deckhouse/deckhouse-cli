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
	"errors"
	"fmt"
	"log/slog"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
)

// ErrExpired is returned by WaitReady when the DataExport enters the Expired
// terminal state and can no longer be used for data transfer.
var ErrExpired = errors.New("DataExport expired")

// defaultDataExportTTL is the fallback TTL used for DataExport when the caller
// passes an empty string. Snapshot transfers can be large, so we use a longer
// default than the 2-minute interactive default.
const defaultDataExportTTL = "2h"

// logEveryN is the poll-attempt cadence at which WaitReady emits a progress log.
// With a 3 s poll interval, every 5 attempts ≈ 15 s.
const logEveryN = 5

// conditionTypeExpired is the producer's condition type set on a DataExport once its
// TTL elapses (storage-volume-data-manager/common/conditions.go). The producer
// deliberately never deletes the CR on expiry, so both EnsureDataExport and WaitReady
// must recognize this exact string.
const conditionTypeExpired = "Expired"

// DataExportName derives a deterministic DataExport CR name from the snapshot
// leaf CR name. The result fits in a DNS-1123 label.
func DataExportName(leafName string) string {
	return "de-" + leafName
}

// EnsureDataExport idempotently creates a DataExport in namespace targeting
// the snapshot leaf CR identified by {group, kind, leafName} with the given
// TTL (empty → "2h"). Returns the DataExport object (newly created or pre-existing).
//
// group and kind must identify a namespaced snapshot CR (e.g.
// "snapshot.storage.k8s.io" / "VolumeSnapshot" for a CSI VolumeSnapshot leaf, or
// the domain group / kind for a domain snapshot CR). The controller routes any
// such targetRef through its kind-agnostic categorySnapshot path.
func EnsureDataExport(
	ctx context.Context,
	c client.Client,
	namespace,
	group,
	resource,
	kind,
	leafName,
	ttl string,
) (*deapi.DataExport, error) {
	deName := DataExportName(leafName)

	existing := new(deapi.DataExport)

	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, existing)
	if err == nil {
		if !meta.IsStatusConditionTrue(existing.Status.Conditions, conditionTypeExpired) {
			return existing, nil
		}

		// The producer never deletes an Expired DataExport on its own (manual operator
		// cleanup is its documented contract), so a stale Expired object from a previous
		// session would otherwise be returned forever, permanently blocking resume. Delete
		// it and fall through to the normal create path below.
		// Delete is not synchronous on a real cluster: the object may still be
		// terminating when the Create below runs, which can race into
		// AlreadyExists (swallowed) and hand the caller back the same stale
		// Expired object on this pass. That is a one-run delay, not a
		// regression — the caller's per-node retry on the next resume attempt
		// (pipeline.Run is best-effort per node) converges once the delete has
		// actually propagated.
		if delErr := c.Delete(ctx, existing); delErr != nil && !kubeerrors.IsNotFound(delErr) {
			return nil, fmt.Errorf("delete expired DataExport %q: %w", deName, delErr)
		}
	} else if !kubeerrors.IsNotFound(err) {
		return nil, fmt.Errorf("get DataExport %q: %w", deName, err)
	}

	if ttl == "" {
		ttl = defaultDataExportTTL
	}

	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
		Spec: deapi.DataexportSpec{
			TTL: ttl,
			TargetRef: deapi.TargetRefSpec{
				Group:    group,
				Resource: resource,
				Kind:     kind,
				Name:     leafName,
			},
		},
	}

	if err := c.Create(ctx, de); err != nil && !kubeerrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create DataExport %q: %w", deName, err)
	}

	// Re-fetch so the returned object carries the server-assigned resource version.
	fetched := new(deapi.DataExport)

	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, fetched); err != nil {
		return nil, fmt.Errorf("get DataExport %q after create: %w", deName, err)
	}

	return fetched, nil
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

// WaitReady polls the DataExport named deName until:
//   - its Ready condition is True and Status.URL is populated → returns the DE,
//   - its Expired condition is True → returns a wrapped ErrExpired,
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
) (*deapi.DataExport, error) {
	var lastStatus string

	for attempt := 0; ; attempt++ {
		de := new(deapi.DataExport)

		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, de); err != nil {
			return nil, fmt.Errorf("get DataExport %q: %w", deName, err)
		}

		if meta.IsStatusConditionTrue(de.Status.Conditions, conditionTypeExpired) {
			return nil, fmt.Errorf("DataExport %s/%s: %w", namespace, deName, ErrExpired)
		}

		if de.Status.URL != "" {
			for _, cond := range de.Status.Conditions {
				if cond.Type == "Ready" && cond.Status == metav1.ConditionTrue {
					return de, nil
				}
			}
		}

		lastStatus = readyConditionStatus(de.Status.Conditions, de.Status.URL != "")

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
			return nil, fmt.Errorf(
				"%w; DataExport status: %s\n\nTo inspect DataExport status, run:\n  d8 k -n %s get dataexport %s -o yaml",
				ctx.Err(), lastStatus, namespace, deName,
			)
		case <-time.After(3 * time.Second):
		}
	}
}

// ReleaseDataExport deletes the DataExport named deName in namespace.
// NotFound is treated as success so the call is idempotent.
func ReleaseDataExport(ctx context.Context, c client.Client, namespace, deName string) error {
	de := new(deapi.DataExport)

	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, de)
	if kubeerrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("get DataExport %q before delete: %w", deName, err)
	}

	if err := c.Delete(ctx, de); err != nil && !kubeerrors.IsNotFound(err) {
		return fmt.Errorf("delete DataExport %q: %w", deName, err)
	}

	return nil
}
