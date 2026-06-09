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

package restore

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	snapshotlog "github.com/deckhouse/deckhouse-cli/internal/snapshot/log"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/restore"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	defaultFieldManager = "d8-snapshot-restore"

	cmdLong = `Restore Kubernetes objects and volume data from a local snapshot archive.

The archive must have been created by "d8 snapshot download".

Restore flow (default):
  1. Build a restore plan from the archive (apply node/object filters).
  2. Print a summary of what will be applied / imported.
  3. Server dry-run all manifests.  On conflict, open $KUBE_EDITOR (or $EDITOR)
     so you can resolve it; dry-run is retried after each save.
  4. If --dry-run is set, stop here — nothing is modified on the cluster.
  5. Apply all manifests via Server-Side Apply.
  6. For each volume (PVC with data): create a DataImport, upload the data
     from the archive, send POST /api/v1/finished, wait for Completed.

Namespace handling:
  All namespaced objects are applied to the target namespace (specified via -n)
  regardless of what namespace they had in the archive. Cluster-scoped objects
  (Namespace, CRD …) are applied as-is. The original metadata.namespace in
  manifests is NOT rewritten on disk.

PVC handling (Mode=All):
  A PVC that has volume data in the archive is NOT applied as a plain manifest.
  Instead, a DataImport is created which provisions the PVC and populates it
  via the volume populator. This is the local equivalent of what the server-side
  manifests-with-data-restoration endpoint does.`

	cmdExample = `  # Restore everything (manifests + volumes) from an archive into the current kubeconfig namespace
  d8 snapshot restore --archive ./ns-snap

  # Restore into a specific namespace
  d8 snapshot restore --archive ./ns-snap -n demo

  # Dry-run only — show what would be applied, without touching the cluster
  d8 snapshot restore --archive ./ns-snap -n demo --dry-run

  # Restore only Kubernetes manifests (skip volume data upload)
  d8 snapshot restore --archive ./ns-snap -n demo --manifests-only

  # Restore only volume data (skip manifest apply)
  d8 snapshot restore --archive ./ns-snap -n demo --data-only

  # Restore only the subtree rooted at a specific node
  d8 snapshot restore --archive ./ns-snap -n demo --node VirtualDiskSnapshot--root

  # Restore a single object
  d8 snapshot restore --archive ./ns-snap -n demo --object apps/v1/Deployment/my-app

  # Skip conflict editor (CI mode) — any conflict fails immediately
  d8 snapshot restore --archive ./ns-snap -n demo --no-edit

  # Force-resolve all SSA field-ownership conflicts
  d8 snapshot restore --archive ./ns-snap -n demo --force-conflicts

  # Allow restoring from an archive that has no COMPLETE sentinel
  d8 snapshot restore --archive ./ns-snap -n demo --allow-incomplete`
)

// NewCommand returns the cobra command for "d8 snapshot restore".
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "restore",
		Short:   "Restore objects and volumes from a local snapshot archive",
		Long:    cmdLong,
		Example: cmdExample,
		Args:    cobra.NoArgs,
		RunE:    run,
	}

	cmd.Flags().StringP("namespace", "n", "", "target namespace to restore into (default: current kubeconfig namespace)")
	cmd.Flags().String("archive", "", "path to a local archive directory created by `d8 snapshot download` (required)")
	_ = cmd.MarkFlagRequired("archive")

	cmd.Flags().Bool("manifests-only", false, "restore only Kubernetes manifests; skip volume data upload")
	cmd.Flags().Bool("data-only", false, "restore only volume data via DataImport; skip manifest apply")
	cmd.Flags().String("node", "", "restore only the subtree rooted at this node ID (e.g. VirtualDiskSnapshot--root)")
	cmd.Flags().String("object", "", "restore a single object: <apiVersion>/<Kind>/<name>")
	cmd.Flags().Bool("dry-run", false, "validate with server dry-run but do not apply anything")
	cmd.Flags().Bool("no-edit", false, "do not open editor on conflict — fail immediately (useful in CI)")
	cmd.Flags().Bool("force-conflicts", false, "apply --force-conflicts to Server-Side Apply (steal field ownership)")
	cmd.Flags().Bool("allow-incomplete", false, "allow restoring from an archive without a COMPLETE sentinel")
	cmd.Flags().String("ttl", "", "TTL for created DataImport objects (e.g. 1h; default 60m)")
	cmd.Flags().String("field-manager", defaultFieldManager, "SSA field manager name")

	return cmd
}

func run(cmd *cobra.Command, _ []string) error {
	targetNS, _ := cmd.Flags().GetString("namespace")
	if targetNS == "" {
		targetNS = safeClient.DefaultNamespace()
	}

	archiveDir, _ := cmd.Flags().GetString("archive")
	manifestsOnly, _ := cmd.Flags().GetBool("manifests-only")
	dataOnly, _ := cmd.Flags().GetBool("data-only")
	nodeFilter, _ := cmd.Flags().GetString("node")
	objectFilter, _ := cmd.Flags().GetString("object")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	noEdit, _ := cmd.Flags().GetBool("no-edit")
	forceConflicts, _ := cmd.Flags().GetBool("force-conflicts")
	allowIncomplete, _ := cmd.Flags().GetBool("allow-incomplete")
	ttl, _ := cmd.Flags().GetString("ttl")
	fieldManager, _ := cmd.Flags().GetString("field-manager")

	if manifestsOnly && dataOnly {
		return fmt.Errorf("--manifests-only and --data-only are mutually exclusive")
	}

	mode := restore.ModeAll
	switch {
	case manifestsOnly:
		mode = restore.ModeManifestsOnly
	case dataOnly:
		mode = restore.ModeDataOnly
	}

	opts := restore.Options{
		TargetNamespace: targetNS,
		NodeFilter:      nodeFilter,
		ObjectFilter:    objectFilter,
		Mode:            mode,
		DryRun:          dryRun,
		NoEdit:          noEdit,
		ForceConflicts:  forceConflicts,
		AllowIncomplete: allowIncomplete,
		DataImportTTL:   ttl,
		FieldManager:    fieldManager,
	}

	log := snapshotlog.New()

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	// Build the restore plan from the local archive.
	src := restore.ArchiveSource{ArchiveDir: archiveDir}

	plan, err := src.Build(opts)
	if err != nil {
		return fmt.Errorf("build restore plan: %w", err)
	}

	errW := cmd.ErrOrStderr()

	printSummary(plan, errW)

	if len(plan.Manifests) == 0 && len(plan.Volumes) == 0 {
		fmt.Fprintln(errW, "Nothing to restore.")
		return nil
	}

	safeClient.SupportNoAuth = false

	sClient, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("build Kubernetes client: %w", err)
	}

	rtClient, err := sClient.NewRTClient()
	if err != nil {
		return fmt.Errorf("build runtime client: %w", err)
	}

	applier := &restore.Applier{
		Client:       rtClient,
		FieldManager: fieldManager,
		Log:          log,
	}

	// Phase 1: server dry-run (always runs, even without --dry-run flag).
	var resolved []restore.ResolvedOp

	if len(plan.Manifests) > 0 {
		fmt.Fprintln(errW, "\nPhase 1: server dry-run …")

		resolved, err = applier.DryRunPhase(ctx, plan)
		if err != nil {
			return fmt.Errorf("dry-run: %w", err)
		}

		fmt.Fprintf(errW, "  Dry-run: %d manifest(s) OK\n", len(resolved))
	}

	if dryRun {
		fmt.Fprintln(errW, "\nDry-run complete. No changes applied (--dry-run).")
		return nil
	}

	// Phase 2: apply manifests.
	if len(resolved) > 0 {
		fmt.Fprintln(errW, "\nPhase 2: applying manifests …")

		if err := applier.ApplyPhase(ctx, resolved, targetNS, forceConflicts); err != nil {
			return fmt.Errorf("apply manifests: %w", err)
		}

		fmt.Fprintf(errW, "  Applied: %d manifest(s)\n", len(resolved))
	}

	// Phase 3: restore volumes.
	if len(plan.Volumes) > 0 {
		fmt.Fprintf(errW, "\nPhase 3: restoring %d volume(s) via DataImport …\n", len(plan.Volumes))

		volRestorer := &restore.VolumeRestorer{
			SafeClient: sClient,
			TTL:        ttl,
			Log:        log,
		}

		for i, vol := range plan.Volumes {
			fmt.Fprintf(errW, "  [%d/%d] %s (PVC: %s, mode: %s)\n",
				i+1, len(plan.Volumes), vol.VSCName, vol.PVCName, vol.VolumeMode)

			if err := volRestorer.Restore(ctx, vol, targetNS); err != nil {
				return fmt.Errorf("restore volume %s: %w", vol.VSCName, err)
			}
		}

		fmt.Fprintf(errW, "  Volumes restored: %d\n", len(plan.Volumes))
	}

	fmt.Fprintln(errW, "\nRestore complete.")

	return nil
}

// printSummary writes a human-readable plan summary to w.
func printSummary(plan *restore.RestorePlan, w io.Writer) {
	fmt.Fprintln(w, "\nRestore plan")
	fmt.Fprintf(w, "  Archive  : %s\n", plan.ArchiveDir)

	if plan.Meta.Source.RootSnapshot.Name != "" {
		fmt.Fprintf(w, "  Snapshot : %s/%s\n",
			plan.Meta.Source.Namespace, plan.Meta.Source.RootSnapshot.Name)
	}

	fmt.Fprintf(w, "  Target NS: %s\n", plan.Opts.TargetNamespace)

	modeStr := "all (manifests + volumes)"
	switch plan.Opts.Mode {
	case restore.ModeManifestsOnly:
		modeStr = "manifests only"
	case restore.ModeDataOnly:
		modeStr = "volume data only"
	}

	fmt.Fprintf(w, "  Mode     : %s\n", modeStr)
	fmt.Fprintf(w, "\nManifests to apply : %d\n", len(plan.Manifests))

	for _, op := range plan.Manifests {
		ns := op.Namespace
		if plan.Opts.TargetNamespace != "" && ns != "" {
			ns = plan.Opts.TargetNamespace
		}

		fmt.Fprintf(w, "  %-30s %s/%s\n", op.Kind, ns, op.Name)
	}

	fmt.Fprintf(w, "\nVolumes to import  : %d\n", len(plan.Volumes))

	for _, vol := range plan.Volumes {
		size := restore.BytesToStorage(vol.BytesTotal)
		if vol.PVCSpec != nil && vol.PVCSpec.StorageRequest != "" {
			size = vol.PVCSpec.StorageRequest
		}

		fmt.Fprintf(w, "  %-30s PVC: %-20s (%s, %s)\n",
			vol.VSCName, vol.PVCName, vol.VolumeMode, size)
	}

	fmt.Fprintln(w)
}
