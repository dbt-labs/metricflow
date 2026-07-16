#!/usr/bin/env python3
"""Validate that the Nuitka binary produces identical SQL to the Python interpreter.

Sends the same explain request to both and diffs the SQL field of the response.
Run via: hatch run nuitka-build:validate
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_MANIFEST = "metricflow_semantics/test_helpers/semantic_manifest_yamls/sg_00_minimal_manifest"
_INPUT = (
    json.dumps(
        {
            "id": "1",
            "method": "explain",
            "protocol_version": 1,
            "params": {
                "manifest_path": _MANIFEST,
                "metric_names": ["bookings"],
                "group_by_names": ["metric_time"],
                "sql_engine": "DUCKDB",
            },
        }
    )
    + "\n"
    + json.dumps({"id": "2", "method": "shutdown", "protocol_version": 1})
    + "\n"
)

_BIN = Path("sidecar/mf_entry.dist") / ("mf_entry.bin" if sys.platform != "win32" else "mf_entry.exe")


def get_sql(cmd: list[str]) -> str:
    """Run cmd, send IPC requests via stdin, return the SQL from the explain response."""
    result = subprocess.run(cmd, input=_INPUT, capture_output=True, text=True)
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"Unexpected output from {cmd}:\nstdout: {result.stdout}\nstderr: {result.stderr}")
    resp = json.loads(lines[1])
    if not resp.get("ok"):
        raise RuntimeError(f"explain failed: {resp}")
    return str(resp["sql"])


if __name__ == "__main__":
    if not _BIN.exists():
        print(f"ERROR: binary not found at {_BIN} — run 'hatch run nuitka-build:compile' first", file=sys.stderr)
        sys.exit(1)

    print("Generating reference SQL with Python interpreter...")
    py_sql = get_sql([sys.executable, "sidecar/mf_entry.py"])

    print(f"Generating SQL with Nuitka binary ({_BIN})...")
    bin_sql = get_sql([str(_BIN)])

    if py_sql != bin_sql:
        print("FAIL: SQL mismatch", file=sys.stderr)
        print(f"\nPython output:\n{py_sql}", file=sys.stderr)
        print(f"\nBinary output:\n{bin_sql}", file=sys.stderr)
        sys.exit(1)

    print("OK: Python and Nuitka binary produce identical SQL")
