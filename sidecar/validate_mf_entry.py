#!/usr/bin/env python3
"""Validate that the Nuitka binary behaves identically to the Python interpreter.

Sends the same requests to both and diffs the responses: the SQL from `explain`, and the full payload from
`validate` for both a valid manifest and a deliberately broken one (a valid manifest alone would only compare two
empty issue lists).
Run via: hatch run nuitka-build:validate
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

_MANIFEST = "metricflow_semantics/test_helpers/semantic_manifest_yamls/sg_00_minimal_manifest"
_BIN = Path("sidecar/mf_entry.dist") / ("mf_entry.bin" if sys.platform != "win32" else "mf_entry.exe")


def _request(req_id: str, method: str, params: dict[str, object] | None = None) -> str:
    envelope: dict[str, object] = {"id": req_id, "method": method, "protocol_version": 1}
    if params is not None:
        envelope["params"] = params
    return json.dumps(envelope) + "\n"


def run_session(cmd: list[str], requests: list[str]) -> list[dict[str, object]]:
    """Run cmd, send the requests then shutdown via stdin, and return one parsed response per request."""
    stdin = "".join(requests) + _request("shutdown", "shutdown")
    result = subprocess.run(cmd, input=stdin, capture_output=True, text=True)
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    # Line 0 is the ready message; then one response per request, then the shutdown response.
    if len(lines) != len(requests) + 2:
        raise RuntimeError(f"Unexpected output from {cmd}:\nstdout: {result.stdout}\nstderr: {result.stderr}")
    return [json.loads(line) for line in lines[1 : 1 + len(requests)]]


def make_broken_manifest(parent: Path) -> Path:
    """Copy the minimal manifest into parent, pointing its metric at a measure that does not exist."""
    broken = parent / "broken_manifest"
    shutil.copytree(_MANIFEST, broken)
    manifest_yaml = broken / "manifest.yaml"
    contents = manifest_yaml.read_text()
    assert "measure: booking_count" in contents
    manifest_yaml.write_text(contents.replace("measure: booking_count", "measure: no_such_measure"))
    return broken


def collect_results(cmd: list[str], broken_manifest: Path) -> dict[str, dict[str, object]]:
    """Run every request this script compares, returning each response keyed by a label (ids are label-derived)."""
    requests = {
        "explain": _request(
            "explain",
            "explain",
            {
                "manifest_path": _MANIFEST,
                "metric_names": ["bookings"],
                "group_by_names": ["metric_time"],
                "sql_engine": "DUCKDB",
            },
        ),
        "validate_valid": _request("validate_valid", "validate", {"manifest_path": _MANIFEST}),
        "validate_broken": _request("validate_broken", "validate", {"manifest_path": str(broken_manifest)}),
    }
    responses = run_session(cmd, list(requests.values()))
    return dict(zip(requests, responses))


def check_expectations(label: str, resp: dict[str, object]) -> None:
    """Guard against both sides agreeing on the wrong answer, e.g. both failing identically."""
    if not resp.get("ok"):
        raise RuntimeError(f"{label} failed: {resp}")
    if label == "validate_valid" and resp["valid"] is not True:
        raise RuntimeError(f"Expected the minimal manifest to be valid, got: {resp}")
    if label == "validate_broken" and (resp["valid"] is not False or not resp["issues"]):
        raise RuntimeError(f"Expected the broken manifest to be invalid with issues, got: {resp}")


if __name__ == "__main__":
    if not _BIN.exists():
        print(f"ERROR: binary not found at {_BIN} — run 'hatch run nuitka-build:compile' first", file=sys.stderr)
        sys.exit(1)

    with tempfile.TemporaryDirectory() as tmp:
        broken_manifest = make_broken_manifest(Path(tmp))

        print("Generating reference responses with Python interpreter...")
        py_results = collect_results([sys.executable, "sidecar/mf_entry.py"], broken_manifest)

        print(f"Generating responses with Nuitka binary ({_BIN})...")
        bin_results = collect_results([str(_BIN)], broken_manifest)

    failed = False
    for label, py_resp in py_results.items():
        check_expectations(label, py_resp)
        bin_resp = bin_results[label]
        if py_resp != bin_resp:
            failed = True
            print(f"FAIL: {label} mismatch", file=sys.stderr)
            print(f"\nPython output:\n{json.dumps(py_resp, indent=2)}", file=sys.stderr)
            print(f"\nBinary output:\n{json.dumps(bin_resp, indent=2)}", file=sys.stderr)
        else:
            print(f"OK: {label} identical")

    if failed:
        sys.exit(1)
    print("OK: Python and Nuitka binary produce identical responses")
