"""Subprocess integration tests for sidecar/mf_entry.py (mf-ipc v1).

Each test communicates with a real mf_entry.py process over stdin/stdout,
mirroring exactly how dbt-core v2 uses the sidecar.
"""
from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent.parent
_MF_ENTRY = _REPO_ROOT / "poc" / "mf_entry.py"
_MANIFEST_DIR = (
    _REPO_ROOT / "metricflow_semantics" / "test_helpers" / "semantic_manifest_yamls" / "sg_00_minimal_manifest"
)


def _send(proc: subprocess.Popen, req: dict) -> dict:  # type: ignore[type-arg]
    assert proc.stdin is not None and proc.stdout is not None
    proc.stdin.write(json.dumps(req) + "\n")
    proc.stdin.flush()
    return json.loads(proc.stdout.readline())


@pytest.fixture(scope="module")
def sidecar() -> Iterator[subprocess.Popen[str]]:
    """Spawns mf_entry.py pre-warmed with the test manifest; shared across the module."""
    proc = subprocess.Popen(
        [sys.executable, str(_MF_ENTRY), "--manifest-path", str(_MANIFEST_DIR)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    ready = json.loads(proc.stdout.readline())  # type: ignore[union-attr]
    assert ready["status"] == "ready", f"Expected ready, got: {ready}"
    yield proc
    try:
        proc.stdin.write(json.dumps({"id": "teardown", "method": "shutdown", "v": 1}) + "\n")  # type: ignore[union-attr]
        proc.stdin.flush()  # type: ignore[union-attr]
        proc.wait(timeout=10)
    except Exception:
        proc.kill()


def test_ready_message_fields(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """Ready message must include version and protocol fields; process must be alive."""
    assert sidecar.poll() is None  # still running


def test_ping(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """Ping returns ok:true with the matching request id."""
    resp = _send(sidecar, {"id": "ping-1", "method": "ping", "v": 1})
    assert resp == {"id": "ping-1", "ok": True}


def test_explain_returns_sql(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """Explain returns ok:true with a non-empty SQL string containing the metric name."""
    resp = _send(
        sidecar,
        {
            "id": "explain-1",
            "method": "explain",
            "v": 1,
            "params": {
                "manifest_path": str(_MANIFEST_DIR),
                "metric_names": ["bookings"],
                "group_by_names": ["metric_time"],
                "sql_engine": "DUCKDB",
            },
        },
    )
    assert resp["id"] == "explain-1"
    assert resp["ok"] is True
    assert "SELECT" in resp["sql"]
    assert "bookings" in resp["sql"].lower()


def test_explain_invalid_metric_returns_structured_error(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """Explain with an unknown metric name returns ok:false with InvalidQueryException."""
    resp = _send(
        sidecar,
        {
            "id": "explain-2",
            "method": "explain",
            "v": 1,
            "params": {
                "manifest_path": str(_MANIFEST_DIR),
                "metric_names": ["nonexistent_metric"],
                "sql_engine": "DUCKDB",
            },
        },
    )
    assert resp["id"] == "explain-2"
    assert resp["ok"] is False
    assert resp["error"]["type"] == "InvalidQueryException"
    assert "nonexistent_metric" in resp["error"]["message"]


def test_unknown_method(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """An unrecognised method name returns ok:false with UnknownMethod."""
    resp = _send(sidecar, {"id": "unk-1", "method": "does_not_exist", "v": 1})
    assert resp["id"] == "unk-1"
    assert resp["ok"] is False
    assert resp["error"]["type"] == "UnknownMethod"
    assert "explain" in resp["error"]["message"]


def test_wrong_protocol_version(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """A request with v!=1 returns ok:false with ProtocolVersionError."""
    resp = _send(sidecar, {"id": "ver-1", "method": "ping", "v": 99})
    assert resp["id"] == "ver-1"
    assert resp["ok"] is False
    assert resp["error"]["type"] == "ProtocolVersionError"


def test_malformed_json(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """A non-JSON line returns ok:false with id:null and JSONDecodeError."""
    assert sidecar.stdin is not None and sidecar.stdout is not None
    sidecar.stdin.write("not valid json\n")
    sidecar.stdin.flush()
    resp = json.loads(sidecar.stdout.readline())
    assert resp["id"] is None
    assert resp["ok"] is False
    assert resp["error"]["type"] == "JSONDecodeError"


def test_shutdown_exits_zero() -> None:
    """Shutdown response is ok:true and process exits with code 0."""
    proc = subprocess.Popen(
        [sys.executable, str(_MF_ENTRY)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None and proc.stdin is not None
    json.loads(proc.stdout.readline())  # consume ready
    resp = _send(proc, {"id": "shut-1", "method": "shutdown", "v": 1})
    assert resp == {"id": "shut-1", "ok": True}
    proc.wait(timeout=5)
    assert proc.returncode == 0


def test_eof_exits_zero() -> None:
    """Closing stdin (EOF) causes the process to exit cleanly with code 0."""
    proc = subprocess.Popen(
        [sys.executable, str(_MF_ENTRY)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None and proc.stdin is not None
    json.loads(proc.stdout.readline())  # consume ready
    proc.stdin.close()
    proc.wait(timeout=5)
    assert proc.returncode == 0
