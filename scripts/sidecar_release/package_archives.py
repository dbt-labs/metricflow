"""Package per-platform mf_entry.dist/ artifact directories into release archives.

Replaces the "Package archives and generate checksums" inline bash step in
cd-build-sidecar-binaries.yaml (DI-4871), so a contributor can run the exact same logic
locally against a `hatch run nuitka-build:build-and-validate` output without needing CI.

Expects `--dist-dir` to contain one `mf_entry-<target-triple>/` directory per platform
(matching the `actions/download-artifact` pattern used in CI, or a single local build
renamed to that shape). Produces `mf_entry-<version>-<target-triple>.tar.gz` (`.zip` for
Windows triples) for each, plus a `SHA256SUMS.txt` covering all of them.

`--tag` is the raw git tag (or ref name) this release was cut from, e.g.
`mf-stdio-sidecar/v0.208.0+2607281534.a1b2c3d4`. Deliberately prefix-agnostic: the archive
version is just everything after the tag's last `/`, so this script doesn't need to change
if the tag namespace is ever renamed again -- only the workflow's trigger/publish-gate
patterns would.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import tarfile
import zipfile
from pathlib import Path


def archive_version_from_tag(tag: str) -> str:
    """Return the version component used in archive filenames for a given tag."""
    return tag.rsplit("/", 1)[-1]


def _add_directory_contents_to_tar(tar: tarfile.TarFile, source_dir: Path) -> None:
    for item in sorted(source_dir.iterdir()):
        tar.add(item, arcname=item.name, recursive=True)


def _add_directory_contents_to_zip(zip_file: zipfile.ZipFile, source_dir: Path) -> None:
    for path in sorted(source_dir.rglob("*")):
        if path.is_file():
            zip_file.write(path, arcname=path.relative_to(source_dir))


def package_platform_directory(platform_dir: Path, version: str, dist_dir: Path) -> Path:
    """Archive one `mf_entry-<triple>/` directory and return the produced archive path."""
    triple = platform_dir.name.removeprefix("mf_entry-")
    is_windows = "windows" in triple

    if is_windows:
        archive_path = dist_dir / f"mf_entry-{version}-{triple}.zip"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            _add_directory_contents_to_zip(zip_file, platform_dir)
    else:
        archive_path = dist_dir / f"mf_entry-{version}-{triple}.tar.gz"
        with tarfile.open(archive_path, "w:gz") as tar:
            _add_directory_contents_to_tar(tar, platform_dir)

    return archive_path


def write_checksums(archive_paths: list[Path], output_path: Path) -> None:
    """Write a SHA256SUMS.txt-style checksum file covering the given archives."""
    lines = []
    for archive_path in sorted(archive_paths):
        digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {archive_path.name}\n")
    output_path.write_text("".join(lines))


def package_release_archives(dist_dir: Path, tag: str) -> list[Path]:
    """Package every `mf_entry-<triple>/` directory under `dist_dir` and write checksums."""
    version = archive_version_from_tag(tag)
    platform_dirs = sorted(p for p in dist_dir.iterdir() if p.is_dir() and p.name.startswith("mf_entry-"))
    if not platform_dirs:
        raise RuntimeError(f"No mf_entry-<triple>/ directories found under {dist_dir}")

    archive_paths = []
    for platform_dir in platform_dirs:
        archive_path = package_platform_directory(platform_dir, version, dist_dir)
        print(f"Packaged {platform_dir.name} -> {archive_path.name}")
        archive_paths.append(archive_path)
        shutil.rmtree(platform_dir)

    write_checksums(archive_paths, dist_dir / "SHA256SUMS.txt")
    return archive_paths


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dist-dir", type=Path, default=Path("dist"), help="Directory containing mf_entry-<triple>/ dirs."
    )
    parser.add_argument("--tag", required=True, help="The git tag (or ref name) this release was cut from.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:  # noqa: D103
    args = _parse_args(argv)
    package_release_archives(args.dist_dir, args.tag)


if __name__ == "__main__":
    main()
