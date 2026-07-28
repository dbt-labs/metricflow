from __future__ import annotations

import tarfile
import zipfile
from pathlib import Path

import pytest

from scripts.sidecar_release.package_archives import (
    archive_version_from_tag,
    package_release_archives,
)


@pytest.mark.parametrize(
    ("tag", "expected_version"),
    [
        ("sidecar/v0.208.0+2", "v0.208.0+2"),
        ("mf-entry-bin/v0.208.0+2607281534.a1b2c3d4", "v0.208.0+2607281534.a1b2c3d4"),
        ("v0.208.0", "v0.208.0"),
    ],
)
def test_archive_version_from_tag_is_prefix_agnostic(tag: str, expected_version: str) -> None:
    """The archive version should be whatever follows the tag's last `/`, regardless of prefix."""
    assert archive_version_from_tag(tag) == expected_version


def _make_platform_dir(dist_dir: Path, triple: str) -> Path:
    platform_dir = dist_dir / f"mf_entry-{triple}"
    (platform_dir / "nested").mkdir(parents=True)
    (platform_dir / "mf_entry.bin").write_text("fake binary contents")
    (platform_dir / "nested" / "extension.so").write_text("fake extension contents")
    return platform_dir


def test_package_release_archives_produces_tar_gz_for_non_windows(tmp_path: Path) -> None:
    """Non-Windows triples should be packaged as a flat .tar.gz with no wrapper directory."""
    dist_dir = tmp_path / "dist"
    _make_platform_dir(dist_dir, "x86_64-unknown-linux-gnu")

    archive_paths = package_release_archives(dist_dir, tag="mf-entry-bin/v0.208.0+2")

    assert len(archive_paths) == 1
    archive_path = archive_paths[0]
    assert archive_path.name == "mf_entry-v0.208.0+2-x86_64-unknown-linux-gnu.tar.gz"

    with tarfile.open(archive_path) as tar:
        names = sorted(tar.getnames())
    # No wrapper directory -- extracting reproduces mf_entry.dist/'s own contents at the root.
    assert names == ["mf_entry.bin", "nested", "nested/extension.so"]


def test_package_release_archives_produces_zip_for_windows(tmp_path: Path) -> None:
    """Windows triples should be packaged as a flat .zip with no wrapper directory."""
    dist_dir = tmp_path / "dist"
    _make_platform_dir(dist_dir, "x86_64-pc-windows-msvc")

    archive_paths = package_release_archives(dist_dir, tag="mf-entry-bin/v0.208.0+2")

    assert len(archive_paths) == 1
    archive_path = archive_paths[0]
    assert archive_path.name == "mf_entry-v0.208.0+2-x86_64-pc-windows-msvc.zip"

    with zipfile.ZipFile(archive_path) as zip_file:
        names = sorted(zip_file.namelist())
    assert names == ["mf_entry.bin", "nested/extension.so"]


def test_package_release_archives_writes_checksums_for_every_archive(tmp_path: Path) -> None:
    """SHA256SUMS.txt should list every produced archive, one line each."""
    dist_dir = tmp_path / "dist"
    _make_platform_dir(dist_dir, "aarch64-apple-darwin")
    _make_platform_dir(dist_dir, "x86_64-pc-windows-msvc")

    package_release_archives(dist_dir, tag="mf-entry-bin/v0.208.0+2")

    checksums = (dist_dir / "SHA256SUMS.txt").read_text()
    assert "mf_entry-v0.208.0+2-aarch64-apple-darwin.tar.gz" in checksums
    assert "mf_entry-v0.208.0+2-x86_64-pc-windows-msvc.zip" in checksums
    assert len(checksums.strip().splitlines()) == 2


def test_package_release_archives_removes_source_directories(tmp_path: Path) -> None:
    """The source mf_entry-<triple>/ directory should be removed once it's archived."""
    dist_dir = tmp_path / "dist"
    platform_dir = _make_platform_dir(dist_dir, "x86_64-unknown-linux-gnu")

    package_release_archives(dist_dir, tag="mf-entry-bin/v0.208.0+2")

    assert not platform_dir.exists()


def test_package_release_archives_raises_when_nothing_to_package(tmp_path: Path) -> None:
    """An empty dist dir should fail loudly rather than silently produce nothing."""
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()

    with pytest.raises(RuntimeError, match="No mf_entry-<triple>/ directories found"):
        package_release_archives(dist_dir, tag="mf-entry-bin/v0.208.0+2")
