from __future__ import annotations

import logging
from dataclasses import dataclass
from difflib import unified_diff
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileDiff:
    """Diff result."""

    relative_path: Path
    diff: str


def diff_matching_files(
    dir_a: Path,
    dir_b: Path,
    glob_pattern: str,
) -> list[FileDiff]:
    """Compare text files matched by `glob_pattern` under both directories.

    Files are matched by the same relative path under each root. Returns only files that actually differ.
    """
    files_a = {p.relative_to(dir_a): p for p in dir_a.glob(glob_pattern) if p.is_file()}
    files_b = {p.relative_to(dir_b): p for p in dir_b.glob(glob_pattern) if p.is_file()}

    common_paths = sorted(set(files_a) & set(files_b))
    diffs: list[FileDiff] = []

    for rel_path in common_paths:
        a_lines = files_a[rel_path].read_text(encoding="utf-8").splitlines(keepends=True)
        b_lines = files_b[rel_path].read_text(encoding="utf-8").splitlines(keepends=True)

        diff_lines = list(
            unified_diff(
                a_lines,
                b_lines,
                fromfile=str(files_a[rel_path]),
                tofile=str(files_b[rel_path]),
            )
        )

        if diff_lines:
            diffs.append(FileDiff(relative_path=rel_path, diff="".join(diff_lines)))

    return diffs
