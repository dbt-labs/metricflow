from __future__ import annotations

import json
from pathlib import Path

from scripts.sidecar_release.generate_build_info import generate_build_info


def test_generate_build_info_contents() -> None:
    """The assembled dict should carry all five fields with the parsed MetricFlow version."""
    build_info = generate_build_info(
        tag="mf-stdio-sidecar/v0.208.0+2607281534.a1b2c3d4",
        commit_sha="deadbeefcafe",
        nuitka_version="4.1.2\n",
        python_version="Python 3.10.19\n",
    )

    assert build_info == {
        "tag": "mf-stdio-sidecar/v0.208.0+2607281534.a1b2c3d4",
        "metricflow_version": "0.208.0",
        "commit": "deadbeefcafe",
        "nuitka_version": "4.1.2",
        "python_version": "Python 3.10.19",
    }


def test_main_writes_valid_json(tmp_path: Path) -> None:
    """The CLI entrypoint should write parseable JSON to the requested output path."""
    from scripts.sidecar_release.generate_build_info import main

    nuitka_version_file = tmp_path / "nuitka-version.txt"
    nuitka_version_file.write_text("4.1.2\n")
    python_version_file = tmp_path / "python-version.txt"
    python_version_file.write_text("Python 3.10.19\n")
    output_path = tmp_path / "build-info.json"

    main(
        [
            "--tag",
            "mf-stdio-sidecar/v0.208.0+2607281534.a1b2c3d4",
            "--commit",
            "deadbeefcafe",
            "--nuitka-version-file",
            str(nuitka_version_file),
            "--python-version-file",
            str(python_version_file),
            "--output",
            str(output_path),
        ]
    )

    written = json.loads(output_path.read_text())
    assert written["metricflow_version"] == "0.208.0"
    assert written["commit"] == "deadbeefcafe"
