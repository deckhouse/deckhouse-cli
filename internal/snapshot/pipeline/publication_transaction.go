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

package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

const (
	publicationTransactionName = ".d8-snapshot-publication-v1.json"
	publicationReceiptName     = ".d8-snapshot-publication-receipt-v1.json"
	publicationStateVersion    = 1
	maxPublicationStateBytes   = 8 << 20
)

var errPublicationTransactionInvalid = errors.New("invalid publication transaction")

type publicationIdentity struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace,omitempty"`
	UID        string `json:"uid,omitempty"`
}

type publicationEntry struct {
	Path                  string                `json:"path"`
	Identity              publicationIdentity   `json:"identity"`
	NodeChecksum          archive.NodeChecksum  `json:"nodeChecksum"`
	ChildrenChecksum      archive.NodeChecksum  `json:"childrenChecksum"`
	HadSnapshot           bool                  `json:"hadSnapshot"`
	PriorSnapshotDigest   string                `json:"priorSnapshotDigest,omitempty"`
	PriorChildrenChecksum *archive.NodeChecksum `json:"priorChildrenChecksum,omitempty"`
}

type publicationTransaction struct {
	Version          int                `json:"version"`
	ArchiveRoot      string             `json:"archiveRoot"`
	SourceTreeDigest string             `json:"sourceTreeDigest"`
	Entries          []publicationEntry `json:"entries"`
	Checksum         string             `json:"checksum"`
}

type publicationReceipt struct {
	Version             int    `json:"version"`
	ArchiveRoot         string `json:"archiveRoot"`
	SourceTreeDigest    string `json:"sourceTreeDigest"`
	TransactionChecksum string `json:"transactionChecksum"`
	Cleaned             bool   `json:"cleaned"`
	Checksum            string `json:"checksum"`
}

func publicationTreeDigest(root *source.Node) (string, error) {
	type record struct {
		Identity publicationIdentity `json:"identity"`
		Parent   publicationIdentity `json:"parent,omitempty"`
	}

	records := make([]record, 0)

	var walk func(*source.Node, *source.Node) error

	walk = func(node, parent *source.Node) error {
		if node == nil {
			return fmt.Errorf("nil source node: %w", errPublicationTransactionInvalid)
		}

		item := record{Identity: publicationIdentityForNode(node)}
		if parent != nil {
			item.Parent = publicationIdentityForNode(parent)
		}

		records = append(records, item)

		for _, child := range node.Children {
			if err := walk(child, node); err != nil {
				return err
			}
		}

		return nil
	}

	if err := walk(root, nil); err != nil {
		return "", err
	}

	sort.Slice(records, func(i, j int) bool {
		return publicationIdentityKey(records[i].Identity) < publicationIdentityKey(records[j].Identity)
	})

	data, err := json.Marshal(records)
	if err != nil {
		return "", fmt.Errorf("marshal source tree identity: %w", err)
	}

	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:]), nil
}

func publicationIdentityForNode(node *source.Node) publicationIdentity {
	return publicationIdentity{
		APIVersion: node.APIVersion,
		Kind:       node.Kind,
		Name:       node.Name,
		Namespace:  node.Namespace,
		UID:        string(node.UID),
	}
}

func publicationIdentityKey(identity publicationIdentity) string {
	return identity.APIVersion + "\x1f" + identity.Kind + "\x1f" +
		identity.Namespace + "\x1f" + identity.Name + "\x1f" + identity.UID
}

func sealPublicationTransaction(transaction *publicationTransaction) error {
	transaction.Checksum = ""

	data, err := json.Marshal(transaction)
	if err != nil {
		return fmt.Errorf("marshal publication transaction: %w", err)
	}

	sum := sha256.Sum256(data)
	transaction.Checksum = hex.EncodeToString(sum[:])

	return nil
}

func validatePublicationTransaction(
	transaction publicationTransaction,
	destination *archive.RootedDestination,
	sourceTreeDigest string,
) error {
	if transaction.Version != publicationStateVersion ||
		transaction.ArchiveRoot != destination.Path() ||
		transaction.SourceTreeDigest != sourceTreeDigest ||
		len(transaction.Entries) == 0 ||
		len(transaction.Entries) > 10_000 {
		return fmt.Errorf("publication transaction header does not match this archive and source tree: %w",
			errPublicationTransactionInvalid)
	}

	storedChecksum := transaction.Checksum
	if err := sealPublicationTransaction(&transaction); err != nil {
		return err
	}

	if storedChecksum == "" || transaction.Checksum != storedChecksum {
		return fmt.Errorf("publication transaction checksum mismatch: %w", errPublicationTransactionInvalid)
	}

	seenPaths := make(map[string]struct{}, len(transaction.Entries))
	seenIdentities := make(map[string]struct{}, len(transaction.Entries))

	for _, entry := range transaction.Entries {
		if !filepath.IsLocal(entry.Path) ||
			entry.Identity.APIVersion == "" || entry.Identity.Kind == "" || entry.Identity.Name == "" ||
			entry.NodeChecksum.Hex == "" || entry.ChildrenChecksum.Hex == "" {
			return fmt.Errorf("publication transaction contains incomplete entry: %w", errPublicationTransactionInvalid)
		}

		if _, exists := seenPaths[entry.Path]; exists {
			return fmt.Errorf("publication transaction repeats path %q: %w", entry.Path, errPublicationTransactionInvalid)
		}

		seenPaths[entry.Path] = struct{}{}

		identityKey := publicationIdentityKey(entry.Identity)
		if _, exists := seenIdentities[identityKey]; exists {
			return fmt.Errorf("publication transaction repeats identity %q: %w",
				identityKey, errPublicationTransactionInvalid)
		}

		seenIdentities[identityKey] = struct{}{}

		if entry.HadSnapshot != (entry.PriorSnapshotDigest != "") {
			return fmt.Errorf("publication transaction prior snapshot evidence is incomplete for %q: %w",
				entry.Path, errPublicationTransactionInvalid)
		}
	}

	return nil
}

func loadPublicationTransaction(
	destination *archive.RootedDestination,
	sourceTreeDigest string,
) (*publicationTransaction, error) {
	path := publicationStatePath(destination, publicationTransactionName)

	data, found, err := readPublicationState(destination, path)
	if err != nil || !found {
		return nil, err
	}

	var transaction publicationTransaction
	if err := json.Unmarshal(data, &transaction); err != nil {
		return nil, fmt.Errorf("decode publication transaction: %w: %w", err, errPublicationTransactionInvalid)
	}

	if err := validatePublicationTransaction(transaction, destination, sourceTreeDigest); err != nil {
		return nil, err
	}

	return &transaction, nil
}

func readPublicationState(
	destination *archive.RootedDestination,
	path string,
) ([]byte, bool, error) {
	file, err := destination.OpenRegularFile(path, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()

		return nil, false, statErr
	}

	if info.Size() > maxPublicationStateBytes {
		_ = file.Close()

		return nil, false, fmt.Errorf("publication state exceeds %d bytes: %w",
			maxPublicationStateBytes, errPublicationTransactionInvalid)
	}

	data, readErr := io.ReadAll(io.LimitReader(file, maxPublicationStateBytes+1))
	closeErr := file.Close()

	if len(data) > maxPublicationStateBytes {
		return nil, false, fmt.Errorf("publication state exceeds %d bytes: %w",
			maxPublicationStateBytes, errPublicationTransactionInvalid)
	}

	return data, true, errors.Join(readErr, closeErr)
}

func writePublicationTransaction(
	ctx context.Context,
	destination *archive.RootedDestination,
	transaction *publicationTransaction,
) error {
	if err := sealPublicationTransaction(transaction); err != nil {
		return err
	}

	data, err := json.Marshal(transaction)
	if err != nil {
		return fmt.Errorf("marshal publication transaction: %w", err)
	}

	if len(data) > maxPublicationStateBytes {
		return fmt.Errorf("publication transaction exceeds %d bytes: %w",
			maxPublicationStateBytes, errPublicationTransactionInvalid)
	}

	path := publicationStatePath(destination, publicationTransactionName)
	if err := destination.EnsureDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("prepare publication state directory: %w", err)
	}

	return archive.WriteFileAtomicRooted(
		ctx,
		destination,
		path,
		bytes.NewReader(data),
	)
}

func publicationStatePath(destination *archive.RootedDestination, name string) string {
	return filepath.Join(destination.Path(), archive.SnapshotsDirName, name)
}

func snapshotDigest(data []byte) string {
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

func buildPublicationTransaction(
	destination *archive.RootedDestination,
	sourceTreeDigest string,
	tasks []nodeTask,
	publish map[*source.Node]bool,
	computeNodeChecksum func(string) (archive.NodeChecksum, error),
) (*publicationTransaction, error) {
	expected := make(map[*source.Node]publicationEntry, len(tasks))

	for i := len(tasks) - 1; i >= 0; i-- {
		task := tasks[i]

		checksum, err := computeNodeChecksum(task.nodeDir)
		if err != nil {
			return nil, fmt.Errorf("compute publication checksum for %s: %w", task.node.DisplayLabel(), err)
		}

		children := make([]archive.ChildCommitment, 0, len(task.node.Children))
		for _, child := range task.node.Children {
			childEntry, ok := expected[child]
			if !ok {
				return nil, fmt.Errorf("publication child %s was not planned before parent: %w",
					child.DisplayLabel(), errPublicationTransactionInvalid)
			}

			children = append(children, archive.ChildCommitment{
				APIVersion:       childEntry.Identity.APIVersion,
				Kind:             childEntry.Identity.Kind,
				Name:             childEntry.Identity.Name,
				Namespace:        childEntry.Identity.Namespace,
				UID:              childEntry.Identity.UID,
				NodeChecksum:     childEntry.NodeChecksum,
				ChildrenChecksum: childEntry.ChildrenChecksum,
			})
		}

		childrenChecksum, err := archive.ComputeChildrenChecksum(children)
		if err != nil {
			return nil, fmt.Errorf("compute intended children commitment for %s: %w",
				task.node.DisplayLabel(), err)
		}

		relative, err := filepath.Rel(destination.Path(), task.nodeDir)
		if err != nil || !filepath.IsLocal(relative) {
			return nil, fmt.Errorf("derive publication path for %s: %w",
				task.node.DisplayLabel(), errPublicationTransactionInvalid)
		}

		expected[task.node] = publicationEntry{
			Path:             relative,
			Identity:         publicationIdentityForNode(task.node),
			NodeChecksum:     checksum,
			ChildrenChecksum: childrenChecksum,
		}
	}

	entries := make([]publicationEntry, 0, len(publish))
	for _, task := range tasks {
		if !publish[task.node] {
			continue
		}

		entry := expected[task.node]
		snapshotPath := filepath.Join(task.nodeDir, archive.SnapshotYAMLName)

		data, found, err := readPublicationState(destination, snapshotPath)
		if err != nil {
			return nil, fmt.Errorf("read prior publication marker for %s: %w", task.node.DisplayLabel(), err)
		}

		if found {
			metadata, readErr := destination.ReadSnapshotYAML(task.nodeDir)
			if readErr != nil {
				return nil, fmt.Errorf("read prior snapshot metadata for %s: %w", task.node.DisplayLabel(), readErr)
			}

			if !publicationIdentityMatches(entry.Identity, metadata) ||
				metadata.Checksum.Hex != entry.NodeChecksum.Hex {
				return nil, fmt.Errorf("prior snapshot marker for %s does not match intended source identity/content: %w",
					task.node.DisplayLabel(), errPublicationTransactionInvalid)
			}

			entry.HadSnapshot = true
			entry.PriorSnapshotDigest = snapshotDigest(data)
			entry.PriorChildrenChecksum = metadata.ChildrenChecksum
		}

		entries = append(entries, entry)
	}

	transaction := &publicationTransaction{
		Version:          publicationStateVersion,
		ArchiveRoot:      destination.Path(),
		SourceTreeDigest: sourceTreeDigest,
		Entries:          entries,
	}
	if err := sealPublicationTransaction(transaction); err != nil {
		return nil, err
	}

	return transaction, nil
}

func publicationIdentityMatches(identity publicationIdentity, metadata archive.SnapshotYAML) bool {
	return identity.APIVersion == metadata.APIVersion &&
		identity.Kind == metadata.Kind &&
		identity.Name == metadata.Name &&
		identity.Namespace == metadata.Namespace &&
		identity.UID == metadata.UID
}

func publicationEntryForNode(
	transaction *publicationTransaction,
	destination *archive.RootedDestination,
	nodeDir string,
) (publicationEntry, bool) {
	relative, err := filepath.Rel(destination.Path(), nodeDir)
	if err != nil {
		return publicationEntry{}, false
	}

	for _, entry := range transaction.Entries {
		if entry.Path == relative {
			return entry, true
		}
	}

	return publicationEntry{}, false
}

func authorizePublicationMismatch(
	destination *archive.RootedDestination,
	transaction *publicationTransaction,
	nodeDir string,
	identity archive.NodeIdentity,
) error {
	entry, ok := publicationEntryForNode(transaction, destination, nodeDir)
	if !ok {
		return fmt.Errorf("stale parent %s is not recorded by publication transaction: %w",
			nodeDir, archive.ErrChildrenChecksumMismatch)
	}

	metadata, err := destination.ReadSnapshotYAML(nodeDir)
	if err != nil {
		return err
	}

	if !publicationIdentityMatches(entry.Identity, metadata) ||
		entry.Identity.APIVersion != identity.APIVersion ||
		entry.Identity.Kind != identity.Kind ||
		entry.Identity.Name != identity.Name ||
		entry.Identity.Namespace != identity.Namespace ||
		entry.Identity.UID != identity.UID ||
		metadata.Checksum.Hex != entry.NodeChecksum.Hex {
		return fmt.Errorf("stale parent %s differs from recorded publication identity/content: %w",
			nodeDir, errPublicationTransactionInvalid)
	}

	data, found, err := readPublicationState(destination, filepath.Join(nodeDir, archive.SnapshotYAMLName))
	if err != nil || !found {
		return errors.Join(err, errPublicationTransactionInvalid)
	}

	currentDigest := snapshotDigest(data)
	currentIsPrior := entry.HadSnapshot && currentDigest == entry.PriorSnapshotDigest

	currentIsIntended := metadata.ChildrenChecksum != nil &&
		metadata.ChildrenChecksum.Hex == entry.ChildrenChecksum.Hex
	if !currentIsPrior && !currentIsIntended {
		return fmt.Errorf("stale parent %s is neither recorded prior nor intended publication: %w",
			nodeDir, errPublicationTransactionInvalid)
	}

	return nil
}

func verifyPublicationEntryContent(
	destination *archive.RootedDestination,
	entry publicationEntry,
	computeNodeChecksum func(string) (archive.NodeChecksum, error),
) error {
	nodeDir := filepath.Join(destination.Path(), entry.Path)

	checksum, err := computeNodeChecksum(nodeDir)
	if err != nil {
		return err
	}

	if checksum.Hex != entry.NodeChecksum.Hex {
		return fmt.Errorf("publication content changed at %s: %w", nodeDir, archive.ErrChecksumMismatch)
	}

	return nil
}

func verifyPublicationEntryEnvelope(
	destination *archive.RootedDestination,
	entry publicationEntry,
) error {
	nodeDir := filepath.Join(destination.Path(), entry.Path)

	metadata, err := destination.ReadSnapshotYAML(nodeDir)
	if err != nil {
		return err
	}

	if !publicationIdentityMatches(entry.Identity, metadata) ||
		metadata.Checksum.Hex != entry.NodeChecksum.Hex ||
		metadata.ChildrenChecksum == nil ||
		metadata.ChildrenChecksum.Hex != entry.ChildrenChecksum.Hex {
		return fmt.Errorf("publication marker at %s does not match transaction: %w",
			nodeDir, errPublicationTransactionInvalid)
	}

	childrenChecksum, err := destination.ComputeNodeChildrenChecksum(nodeDir)
	if err != nil {
		return fmt.Errorf("compute publication children checksum at %s: %w", nodeDir, err)
	}

	if childrenChecksum.Hex != entry.ChildrenChecksum.Hex {
		return fmt.Errorf("publication child set changed at %s: %w",
			nodeDir, archive.ErrChildrenChecksumMismatch)
	}

	return nil
}

func sealPublicationReceipt(receipt *publicationReceipt) error {
	receipt.Checksum = ""

	data, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("marshal publication receipt: %w", err)
	}

	sum := sha256.Sum256(data)
	receipt.Checksum = hex.EncodeToString(sum[:])

	return nil
}

func loadPublicationReceipt(
	destination *archive.RootedDestination,
	sourceTreeDigest string,
) (*publicationReceipt, error) {
	path := publicationStatePath(destination, publicationReceiptName)

	data, found, err := readPublicationState(destination, path)
	if err != nil || !found {
		return nil, err
	}

	var receipt publicationReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		return nil, fmt.Errorf("decode publication receipt: %w: %w", err, errPublicationTransactionInvalid)
	}

	storedChecksum := receipt.Checksum
	if err := sealPublicationReceipt(&receipt); err != nil {
		return nil, err
	}

	if receipt.Version != publicationStateVersion ||
		receipt.ArchiveRoot != destination.Path() ||
		receipt.SourceTreeDigest != sourceTreeDigest ||
		storedChecksum == "" ||
		receipt.Checksum != storedChecksum {
		return nil, fmt.Errorf("publication receipt does not match this archive/source tree: %w",
			errPublicationTransactionInvalid)
	}

	receipt.Checksum = storedChecksum

	return &receipt, nil
}

func writePublicationReceipt(
	ctx context.Context,
	destination *archive.RootedDestination,
	receipt *publicationReceipt,
) error {
	if err := sealPublicationReceipt(receipt); err != nil {
		return err
	}

	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}

	return archive.WriteFileAtomicRooted(
		ctx,
		destination,
		publicationStatePath(destination, publicationReceiptName),
		bytes.NewReader(data),
	)
}

func completePublicationTransaction(
	ctx context.Context,
	destination *archive.RootedDestination,
	transaction *publicationTransaction,
) error {
	for _, entry := range transaction.Entries {
		if err := verifyPublicationEntryEnvelope(destination, entry); err != nil {
			return fmt.Errorf("verify completed publication %s: %w", entry.Path, err)
		}
	}

	receipt := &publicationReceipt{
		Version:             publicationStateVersion,
		ArchiveRoot:         transaction.ArchiveRoot,
		SourceTreeDigest:    transaction.SourceTreeDigest,
		TransactionChecksum: transaction.Checksum,
	}
	if err := writePublicationReceipt(ctx, destination, receipt); err != nil {
		return fmt.Errorf("publish publication receipt: %w", err)
	}

	transactionPath := publicationStatePath(destination, publicationTransactionName)
	if err := destination.Remove(transactionPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove publication transaction: %w", err)
	}

	if err := destination.SyncParent(transactionPath); err != nil {
		return fmt.Errorf("confirm publication transaction cleanup: %w", err)
	}

	receipt.Cleaned = true
	if err := writePublicationReceipt(ctx, destination, receipt); err != nil {
		return fmt.Errorf("confirm publication cleanup receipt: %w", err)
	}

	return nil
}
