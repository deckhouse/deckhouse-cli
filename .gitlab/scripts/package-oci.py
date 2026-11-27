#!/usr/bin/env python3
"""Build the d8 release index for publication with crane push --index."""

import gzip
import hashlib
import json
import sys
import tarfile
import tempfile
from pathlib import Path


OCI = "application/vnd.oci.image."
PLATFORMS = [("linux", "amd64"), ("darwin", "amd64"), ("darwin", "arm64"), ("windows", "amd64")]


# Expose the layout only after all four platform images are complete.
def build_release_layout(tag, dist, output):
    with tempfile.TemporaryDirectory(dir=output.parent) as tmp:
        layout = Path(tmp) / "layout"
        (layout / "blobs" / "sha256").mkdir(parents=True)
        index = {
            "schemaVersion": 2,
            "mediaType": OCI + "index.v1+json",
            "manifests": [build_platform_manifest(layout, dist, tag, os_name, arch) for os_name, arch in PLATFORMS],
            "annotations": {"org.opencontainers.image.version": tag},
        }
        (layout / "index.json").write_text(json.dumps(index, separators=(",", ":")), encoding="utf-8")
        (layout / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}', encoding="utf-8")
        layout.rename(output)


# The index uses OS and architecture to select the matching d8 binary.
def build_platform_manifest(layout, dist, tag, os_name, arch):
    platform_dir = f"{os_name}-{arch}"
    archive = dist / f"d8-{tag}-{platform_dir}.tar.gz"
    verify_archive_checksum(archive)
    binary_name = "d8.exe" if os_name == "windows" else "d8"
    layer, diff_id = pack_binary_layer(layout, archive, f"{platform_dir}/bin/{binary_name}")

    platform = {"os": os_name, "architecture": arch}
    config = {**platform, "rootfs": {"type": "layers", "diff_ids": [diff_id]}, "config": {}}
    manifest = {
        "schemaVersion": 2,
        "mediaType": OCI + "manifest.v1+json",
        "config": store_oci_json(layout, config, "config.v1+json"),
        "layers": [layer],
    }
    descriptor = store_oci_json(layout, manifest, "manifest.v1+json")
    descriptor["platform"] = platform
    return descriptor


# The checksum file must identify both the archive's contents and its name.
def verify_archive_checksum(archive):
    checksum = archive.with_name(archive.name + ".sha256sum")
    with archive.open("rb") as stream:
        if checksum.read_text().split() != [stream_sha256(stream), archive.name]:
            raise ValueError(f"invalid checksum for {archive}")


# RPP serves a layer containing one executable named d8 on every platform.
def pack_binary_layer(layout, archive, member_name):
    layer = layout / "layer.tar.gz"
    with tarfile.open(archive, "r|gz") as source:
        member = next((entry for entry in source if entry.name == member_name), None)
        if member is None or not member.isfile() or member.size == 0:
            raise ValueError(f"expected a regular binary {member_name} in {archive}")

        entry = tarfile.TarInfo("d8")
        entry.mode = 0o755
        entry.size = member.size
        with gzip.GzipFile(layer, "wb", mtime=0, compresslevel=6) as gzip_layer:
            with tarfile.open(fileobj=gzip_layer, mode="w|", format=tarfile.USTAR_FORMAT) as target:
                target.addfile(entry, source.extractfile(member))

    # OCI uses the uncompressed tar digest as the DiffID.
    with gzip.open(layer, "rb") as stream:
        diff_id = "sha256:" + stream_sha256(stream)
    return store_oci_blob(layout, layer, "layer.v1.tar+gzip"), diff_id


# OCI digests cover the serialized JSON, including whitespace.
def store_oci_json(layout, value, media_type):
    metadata = layout / "metadata.json"
    metadata.write_text(json.dumps(value, separators=(",", ":")), encoding="utf-8")
    return store_oci_blob(layout, metadata, media_type)


# OCI references each stored blob by digest and size.
def store_oci_blob(layout, path, media_type):
    with path.open("rb") as stream:
        digest = stream_sha256(stream)
    size = path.stat().st_size
    path.replace(layout / "blobs" / "sha256" / digest)
    return {"mediaType": OCI + media_type, "size": size, "digest": "sha256:" + digest}


# Hash large archives and layers in bounded memory.
def stream_sha256(stream):
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit("usage: package-oci.py TAG DIST_DIR OUTPUT_LAYOUT")
    try:
        build_release_layout(sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]))
    except (OSError, ValueError, tarfile.TarError) as error:
        sys.exit(str(error))
