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

// DataExportName derives a deterministic DataExport CR name from the shadow
// VolumeSnapshot name. The result fits in a DNS-1123 label.
func DataExportName(shadowVSName string) string {
	return "de-" + shadowVSName
}

// EnsureDataExport idempotently creates a DataExport in namespace targeting
// the shadow VolumeSnapshot shadowVSName with the given TTL (empty → "2h").
// Returns the DataExport object (newly created or pre-existing).
func EnsureDataExport(
	ctx context.Context,
	c client.Client,
	namespace,
	shadowVSName,
	ttl string,
) (*deapi.DataExport, error) {
	deName := DataExportName(shadowVSName)

	existing := new(deapi.DataExport)

	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, existing)
	if err == nil {
		return existing, nil
	}

	if !kubeerrors.IsNotFound(err) {
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
				Kind: "VolumeSnapshot",
				Name: shadowVSName,
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

// WaitReady polls the DataExport named deName until:
//   - its Ready condition is True and Status.URL is populated → returns the DE,
//   - its Expired condition is True → returns a wrapped ErrExpired,
//   - ctx is cancelled → returns ctx.Err().
//
// The poll interval is 3 s. Callers set a deadline via ctx to bound the wait.
func WaitReady(
	ctx context.Context,
	c client.Client,
	log *slog.Logger,
	namespace,
	deName string,
) (*deapi.DataExport, error) {
	for {
		de := new(deapi.DataExport)

		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deName}, de); err != nil {
			return nil, fmt.Errorf("get DataExport %q: %w", deName, err)
		}

		for _, cond := range de.Status.Conditions {
			if cond.Type == "Expired" && cond.Status == metav1.ConditionTrue {
				return nil, fmt.Errorf("DataExport %s/%s: %w", namespace, deName, ErrExpired)
			}
		}

		if de.Status.URL != "" {
			for _, cond := range de.Status.Conditions {
				if cond.Type == "Ready" && cond.Status == metav1.ConditionTrue {
					return de, nil
				}
			}
		}

		log.Info("waiting for DataExport to be ready",
			slog.String("namespace", namespace),
			slog.String("name", deName))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
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
