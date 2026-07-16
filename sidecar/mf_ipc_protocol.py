"""Pydantic models for the mf-ipc v1 wire protocol.

These give the IPC boundary between Fusion and the MetricFlow sidecar request/
response validation and a single typed source of truth for the message
shapes documented in sidecar/README.md — without changing the wire format
itself. Messages are still NDJSON dicts; these models parse and construct
them.

This is intentionally minimal: it does not introduce a schema artifact,
codegen, or cross-repo drift protection. That heavier structured-contract
work (evaluating gRPC/protobuf, or a JSON Schema shared with Fusion) is
tracked separately in DI-4709.
"""
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict

RequestId = str | int | None


class Method(str, Enum):
    """Method names recognized by _dispatch.

    RequestEnvelope.method stays a plain `str | None` (not this enum): an
    unrecognized method is a valid, expected input that must reach
    _dispatch's own UnknownMethod handling, not fail at parse time.
    """

    EXPLAIN = "explain"
    PING = "ping"
    SHUTDOWN = "shutdown"


class _FrozenModel(BaseModel):
    """Base for all mf-ipc messages: parsed or built once, then only read."""

    model_config = ConfigDict(frozen=True)


class ReadyMessage(_FrozenModel):
    """Startup handshake, written once before any request is read from stdin."""

    status: Literal["ready"] = "ready"
    metricflow_version: str
    python_version: str
    protocol_version: Literal[1] = 1


class StartupErrorMessage(_FrozenModel):
    """Written instead of ReadyMessage if --manifest-path pre-loading fails."""

    status: Literal["error"] = "error"
    type: str
    message: str


class RequestEnvelope(_FrozenModel):
    """Generic shape every request must have, validated before method dispatch.

    `id` is required: the IPC loop writes exactly one response per request
    line, so there is no fire-and-forget "notification" case that would
    justify defaulting it. A request missing `id` fails validation here and
    surfaces to the caller as a structured error (itself with id=None, since
    there's no id to echo back) rather than silently proceeding.

    `method` and `protocol_version` are intentionally left unconstrained (not
    Literal) here: an unknown method or a protocol version other than 1 is a
    valid, expected input that must reach _dispatch's own UnknownMethod /
    ProtocolVersionError handling, not fail at the envelope-parsing stage.
    """

    id: RequestId
    method: str | None = None
    protocol_version: int = 1
    params: dict | None = None


class ExplainParams(_FrozenModel):
    """Params for the `explain` method.

    sql_engine is intentionally a plain string, not the SqlEngine enum:
    mf_entry.py looks it up by enum *member name* (SqlEngine["DUCKDB"]), not
    by enum value (SqlEngine.DUCKDB.value == "DuckDB"). Typing this field as
    SqlEngine directly would make pydantic validate against enum values
    instead, silently changing which strings are accepted over the wire.
    """

    manifest_path: str
    metric_names: tuple[str, ...] | None = None
    group_by_names: tuple[str, ...] | None = None
    where_constraints: tuple[str, ...] | None = None
    order_by_names: tuple[str, ...] | None = None
    limit: int | None = None
    sql_engine: str = "DUCKDB"


class ErrorDetail(_FrozenModel):
    """The `error` payload of an ErrorResponse."""

    type: str
    message: str
    traceback: str | None = None


class ErrorResponse(_FrozenModel):
    """Response shape for any request that fails, regardless of method."""

    id: RequestId
    ok: Literal[False] = False
    error: ErrorDetail


class OkResponse(_FrozenModel):
    """Response for `ping` / `shutdown` — ok:true with no extra payload."""

    id: RequestId
    ok: Literal[True] = True


class ExplainResponse(_FrozenModel):
    """Successful response for the `explain` method."""

    id: RequestId
    ok: Literal[True] = True
    sql: str
