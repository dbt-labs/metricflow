"""Direct unit tests for sidecar/mf_ipc_protocol.py's pydantic models.

Complements sidecar/tests/test_mf_entry.py's subprocess integration tests by
testing the message shapes directly, without spawning a process for each case.
"""
from __future__ import annotations

import pytest
from mf_ipc_protocol import (
    ErrorDetail,
    ErrorResponse,
    ExplainParams,
    ExplainResponse,
    OkResponse,
    ReadyMessage,
    RequestEnvelope,
    StartupErrorMessage,
)
from pydantic import BaseModel, ValidationError


def test_explain_params_requires_manifest_path() -> None:
    """manifest_path has no default; omitting it must raise a field-level ValidationError."""
    with pytest.raises(ValidationError, match="manifest_path"):
        ExplainParams.model_validate({})


def test_explain_params_defaults() -> None:
    """All fields except manifest_path are optional, with sql_engine defaulting to DUCKDB."""
    params = ExplainParams.model_validate({"manifest_path": "/some/path"})
    assert params.sql_engine == "DUCKDB"
    assert params.metric_names is None
    assert params.group_by_names is None
    assert params.where_constraints is None
    assert params.order_by_names is None
    assert params.limit is None


def test_request_envelope_requires_id() -> None:
    """The id field has no default; omitting it must raise a field-level ValidationError."""
    with pytest.raises(ValidationError, match="id"):
        RequestEnvelope.model_validate({})


def test_request_envelope_rejects_null_id() -> None:
    """The id field is typed str | int (not RequestId): an explicit null must be rejected, not just an absent key."""
    with pytest.raises(ValidationError, match="id"):
        RequestEnvelope.model_validate({"id": None})


def test_request_envelope_defaults() -> None:
    """Given only id, method/params default to None and protocol_version defaults to 1."""
    envelope = RequestEnvelope.model_validate({"id": "1"})
    assert envelope.id == "1"
    assert envelope.method is None
    assert envelope.protocol_version == 1
    assert envelope.params is None


def test_request_envelope_accepts_unknown_method_and_version() -> None:
    """Unconstrained so _dispatch's own UnknownMethod/ProtocolVersionError logic can run."""
    envelope = RequestEnvelope.model_validate({"id": "1", "method": "does_not_exist", "protocol_version": 99})
    assert envelope.method == "does_not_exist"
    assert envelope.protocol_version == 99


def test_request_envelope_rejects_non_mapping_input() -> None:
    """A JSON array (or any non-mapping) must raise ValidationError, not crash _dispatch."""
    with pytest.raises(ValidationError):
        RequestEnvelope.model_validate([1, 2, 3])


@pytest.mark.parametrize("model_cls", [RequestEnvelope, ExplainParams])
def test_extra_fields_are_ignored_not_rejected(model_cls: type[BaseModel]) -> None:
    """An older MetricFlow shouldn't reject a newer Fusion's request over an unknown field."""
    payload = {"id": "1", "manifest_path": "/some/path", "some_future_field": "value"}
    model_cls.model_validate(payload)  # must not raise


def test_ready_message_round_trips_through_json() -> None:
    """ReadyMessage survives a model_dump_json -> model_validate_json round trip unchanged."""
    msg = ReadyMessage(metricflow_version="0.1.0", python_version="3.11.0")
    restored = ReadyMessage.model_validate_json(msg.model_dump_json())
    assert restored == msg
    assert restored.status == "ready"
    assert restored.protocol_version == 1


def test_startup_error_message_shape() -> None:
    """StartupErrorMessage dumps to the exact shape documented in sidecar/README.md."""
    msg = StartupErrorMessage(type="ValueError", message="bad manifest")
    assert msg.model_dump() == {"status": "error", "type": "ValueError", "message": "bad manifest"}


def test_error_response_shape() -> None:
    """ErrorResponse dumps to the exact shape documented in sidecar/README.md."""
    resp = ErrorResponse(id="1", error=ErrorDetail(type="UnknownMethod", message="nope"))
    assert resp.model_dump() == {
        "id": "1",
        "ok": False,
        "error": {"type": "UnknownMethod", "message": "nope", "traceback": None},
    }


def test_ok_and_explain_response_ok_field_is_fixed() -> None:
    """Ok is always True on these two response types, regardless of construction order."""
    assert OkResponse(id="1").ok is True
    assert ExplainResponse(id="1", sql="SELECT 1").ok is True
