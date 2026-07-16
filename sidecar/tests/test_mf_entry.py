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

import mf_entry
import pytest
from metricflow_semantics.test_helpers.semantic_manifest_yamls.sg_00_minimal_manifest import SG_00_MINIMAL_MANIFEST
from mf_ipc_protocol import ExplainParams, Method, RequestEnvelope

_MF_ENTRY = Path(mf_entry.__file__)
_MANIFEST_DIR = SG_00_MINIMAL_MANIFEST.directory


def _send(proc: subprocess.Popen, req: RequestEnvelope | dict) -> dict:  # type: ignore[type-arg]
    """Send a request and return the decoded response.

    Accepts a raw dict (in addition to RequestEnvelope) so tests can send
    envelopes that wouldn't validate, e.g. one missing the now-required id.
    """
    assert proc.stdin is not None and proc.stdout is not None
    body = req.model_dump_json() if isinstance(req, RequestEnvelope) else json.dumps(req)
    proc.stdin.write(body + "\n")
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
        _send(proc, RequestEnvelope(id="teardown", method=Method.SHUTDOWN.value))
        proc.wait(timeout=10)
    except Exception:
        proc.kill()


def test_ready_message_fields(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """Ready message must include version and protocol fields; process must be alive."""
    assert sidecar.poll() is None  # still running


def test_ping(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """Ping returns ok:true with the matching request id."""
    resp = _send(sidecar, RequestEnvelope(id="ping-1", method=Method.PING.value))
    assert resp == {"id": "ping-1", "ok": True}


def test_explain_returns_sql(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """Explain returns ok:true with a non-empty SQL string containing the metric name."""
    params = ExplainParams(
        manifest_path=str(_MANIFEST_DIR),
        metric_names=["bookings"],
        group_by_names=["metric_time"],
        sql_engine="DUCKDB",
    )
    resp = _send(sidecar, RequestEnvelope(id="explain-1", method=Method.EXPLAIN.value, params=params.model_dump()))
    assert resp["id"] == "explain-1"
    assert resp["ok"] is True
    assert "SELECT" in resp["sql"]
    assert "bookings" in resp["sql"].lower()


def test_explain_invalid_metric_returns_structured_error(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """Explain with an unknown metric name returns ok:false with InvalidQueryException."""
    params = ExplainParams(manifest_path=str(_MANIFEST_DIR), metric_names=["nonexistent_metric"], sql_engine="DUCKDB")
    resp = _send(sidecar, RequestEnvelope(id="explain-2", method=Method.EXPLAIN.value, params=params.model_dump()))
    assert resp["id"] == "explain-2"
    assert resp["ok"] is False
    assert resp["error"]["type"] == "InvalidQueryException"
    assert "nonexistent_metric" in resp["error"]["message"]


def test_unknown_method(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """An unrecognised method name returns ok:false with UnknownMethod."""
    resp = _send(sidecar, RequestEnvelope(id="unk-1", method="does_not_exist"))
    assert resp["id"] == "unk-1"
    assert resp["ok"] is False
    assert resp["error"]["type"] == "UnknownMethod"
    assert "explain" in resp["error"]["message"]


def test_wrong_protocol_version(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """A request with protocol_version!=1 returns ok:false with ProtocolVersionError."""
    resp = _send(sidecar, RequestEnvelope(id="ver-1", method=Method.PING.value, protocol_version=99))
    assert resp["id"] == "ver-1"
    assert resp["ok"] is False
    assert resp["error"]["type"] == "ProtocolVersionError"


def test_missing_id_returns_structured_error(sidecar: subprocess.Popen) -> None:  # type: ignore[type-arg]
    """A request without an id fails RequestEnvelope validation; the error id falls back to None."""
    resp = _send(sidecar, {"method": Method.PING.value, "protocol_version": 1})
    assert resp["id"] is None
    assert resp["ok"] is False
    assert "id" in resp["error"]["message"]


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
    resp = _send(proc, RequestEnvelope(id="shut-1", method=Method.SHUTDOWN.value))
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
