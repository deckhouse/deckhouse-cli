# Snapshot archive reference vectors

These files are the golden vectors of the snapshot archive format, copied byte for byte from
the state-snapshotter repository:

- source: `api/snapshotarchive/snapshotarchivetest/vectors/` in
  `fox.flant.com/deckhouse/storage/state-snapshotter`;
- commit: `9866ab7b` (format version 4).

state-snapshotter holds the reference implementation of the format (`api/snapshotarchive`).
d8 cannot import it, because a public build cannot fetch that module, so d8 keeps its own
implementation in `internal/snapshot/archive` and `vectors_test.go` checks it against these
files.

| Path | Content |
|------|---------|
| `tree/` | a five-node archive at format version 3 |
| `tree-child-metadata/` | the same archive at format version 4 |
| `tampered/` | version 4 descriptors of that archive with a field edited and resealed; each must fail its parent's children checksum |
| `descriptors/`, `canonical/` | stand-alone descriptors and the exact canonical JSON their metadata checksum is taken over |
| `expected.json` | every expected digest |

Never regenerate or edit these files here. A vector that stops matching means the change breaks
archives that are already written. To refresh them, copy the directory again from
state-snapshotter and update the commit above.
