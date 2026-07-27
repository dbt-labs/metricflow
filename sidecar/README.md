# MetricFlow sidecar

The sidecar is MetricFlow compiled into a standalone native binary by
[Nuitka](https://nuitka.net/). dbt-core v2 spawns it as a
subprocess and communicates with it over NDJSON on stdin/stdout using the
**mf-ipc v1** protocol.

The sidecar's only job is to compile metric queries to SQL without executing
them. It wraps `MetricFlowEngine.explain()`.

## Directory layout

```
sidecar/
  mf_entry.py            IPC server (the file Nuitka compiles)
  mf_ipc_protocol.py     pydantic models for every message shape below
  validate_mf_entry.py   validation helper: compares Python vs binary SQL output
  tests/
    test_mf_entry.py         subprocess integration tests (mf-ipc v1)
    test_mf_ipc_protocol.py  direct unit tests for the pydantic models
```

## Development

Run the integration tests against the Python interpreter (no compilation needed):

```bash
hatch run dev-env:pytest sidecar/tests/ -v
```

Quick smoke test from the repo root:

```bash
MANIFEST=metricflow_semantics/test_helpers/semantic_manifest_yamls/sg_00_minimal_manifest
printf '{"id":"1","method":"explain","protocol_version":1,"params":{"manifest_path":"%s","metric_names":["bookings"],"group_by_names":["metric_time"],"sql_engine":"DUCKDB"}}\n{"id":"2","method":"shutdown","protocol_version":1}\n' "$MANIFEST" \
  | hatch run dev-env:python sidecar/mf_entry.py
```

## Building and local validation

To compile and validate in one step:

```bash
hatch run nuitka-build:build-and-validate
```

Output lands in `sidecar/mf_entry.dist/`. The binary is named `mf_entry.bin`
on macOS/Linux and `mf_entry.exe` on Windows. The validation step sends the
same `explain` request to both the Python interpreter and the compiled binary
and diffs the `sql` field — a mismatch means the Nuitka build is diverging
from the interpreter.

The two steps can also be run separately:

```bash
hatch run nuitka-build:compile    # build only
hatch run nuitka-build:validate   # validate only (binary must already exist)
```

## CI builds and release artifacts

Sidecar binaries have their own release versioning, decoupled from MetricFlow's (DI-4871).
Every sidecar release is tagged `sidecar/v<mf_version>+<counter>` — e.g. `sidecar/v0.208.0+1`
— where `<mf_version>` is the MetricFlow version embedded in the binary and `<counter>`
distinguishes multiple sidecar builds off that same MetricFlow version. This is a **separate
GitHub Release from MetricFlow's own `v<mf_version>` tag** (the one used for the PyPI
publish) — Fusion pins the sidecar tag specifically, not the MetricFlow tag.

**The `+1` baseline.** Every MetricFlow release always gets a matching sidecar build with no
separate manual step:
[`scripts/release_tool/release_step_3.py`](../scripts/release_tool/release_step_3.py)
automatically pushes `sidecar/v<mf_version>+1` alongside MetricFlow's own release tag.

**Ad hoc releases (`+2`, `+3`, ...).** For a sidecar-only change — e.g. a new `mf_entry.py`
entry point — that doesn't correspond to a new MetricFlow version, run
[`Cut Ad Hoc Sidecar Release`](../.github/workflows/cd-cut-adhoc-sidecar-release.yaml) from
the Actions tab (`workflow_dispatch`, dispatchable only from `main`). It resolves the
MetricFlow version reachable from the current commit, computes the next counter for that
version, pushes the tag, and triggers the build.

[`cd-build-sidecar-binaries.yaml`](../.github/workflows/cd-build-sidecar-binaries.yaml)
compiles `mf_entry.py` for every platform Fusion needs and publishes the results as assets
on that sidecar tag's GitHub Release:

| target triple | runner | archive |
|---|---|---|
| `aarch64-apple-darwin` | macos-14 | `mf_entry-<version>-aarch64-apple-darwin.tar.gz` |
| `x86_64-apple-darwin` | `vars.MACOS_RUNNER_INTEL` (macos-15-intel) | `mf_entry-<version>-x86_64-apple-darwin.tar.gz` |
| `x86_64-unknown-linux-gnu` | ubuntu-22.04 | `mf_entry-<version>-x86_64-unknown-linux-gnu.tar.gz` |
| `aarch64-unknown-linux-gnu` | ubuntu-24.04-arm | `mf_entry-<version>-aarch64-unknown-linux-gnu.tar.gz` |
| `x86_64-pc-windows-msvc` | windows-latest | `mf_entry-<version>-x86_64-pc-windows-msvc.zip` |

`<version>` in the archive name is the sidecar tag with its `sidecar/` namespace prefix
stripped — e.g. tag `sidecar/v0.208.0+1` produces `mf_entry-v0.208.0+1-<triple>.tar.gz`. The
namespace's only job is keeping the *tag* from also matching
`cd-push-metricflow-to-pypi.yaml`'s `v[0-9]+.[0-9]+.[0-9]+*` trigger glob, so restating it in
every filename would be redundant.

A `SHA256SUMS.txt` and a `build-info.json` are published alongside the archives.
`build-info.json` records the embedded MetricFlow version, the source commit, and the
Nuitka/Python versions actually measured at build time — the thing to check if a specific
binary's exact provenance is ever in question, since ad hoc releases intentionally don't
guard against `metricflow`/`metricflow_semantics`/`metricflow_semantic_interfaces` having
changed since the last MetricFlow release they're tagged against.

Consumers fetch a specific version at
`https://github.com/dbt-labs/metricflow/releases/download/<sidecar-tag>/<archive>` — no
authentication required, since the repo is public. Each archive extracts to the same layout
as a local `sidecar/mf_entry.dist/` build.

This is the contract Fusion's build tooling depends on — changing the target triple list,
archive naming, tag format, or the checksum/build-info file names is a breaking change from
Fusion's perspective, not just a MetricFlow-internal refactor.

**Re-publishing an existing tag fails loudly; it doesn't overwrite.** The release-asset
publish step sets `overwrite_files: false`, so an accidental re-push of an existing sidecar
tag (a mistyped or reused ad hoc counter, or a force-moved tag) fails CI instead of silently
clobbering already-published binaries.

**Version pins:** binaries are compiled with Nuitka `4.1.2` (pinned in
`pyproject.toml`) against Python 3.10, per `setup-python-env`'s default. Both
are chosen for parity with the Nuitka PoC that was manually validated against
`sg_00_minimal_manifest`, not for any Python-3.10-specific behavior.

**Not covered by this pipeline** (tracked separately — see the parent
epic's other tickets): code signing and notarization for the macOS and
Windows binaries, a musl/Alpine Linux build, and full snapshot-suite
validation across every SQL dialect and metric type. The `validate`
step above only diffs one fixture (`sg_00_minimal_manifest`) against
`DUCKDB` — it's a build-sanity smoke check, not a correctness gate.

## mf-ipc v1 protocol

All messages are newline-delimited JSON (NDJSON). The protocol is strictly
sequential: the caller sends one request and waits for one response before
sending the next.

Every message shape documented below has a corresponding pydantic model in
`mf_ipc_protocol.py`, which `mf_entry.py` validates requests against and
builds responses from. This is a minimal, MetricFlow-internal typing layer —
there's no shared schema artifact or codegen for the Rust side yet. Adopting
a structured cross-repo contract (gRPC/protobuf, or a JSON Schema shared with
Fusion) is tracked separately in DI-4709.

### Startup

On launch the sidecar writes a ready message to stdout:

```json
{"status": "ready", "metricflow_version": "X.Y.Z", "python_version": "3.11.x", "protocol_version": 1}
```

If `--manifest-path` was given and manifest loading fails, the sidecar writes
an error message and exits 1:

```json
{"status": "error", "type": "ExceptionClass", "message": "..."}
```

### Request format

```json
{"id": "<string or int>", "method": "<method>", "protocol_version": 1, "params": {...}}
```

`id` is echoed back in the response. `protocol_version` must be `1`.

### Methods

#### `explain`

Compiles a metric query to SQL without executing it.

```json
{
  "id": "1",
  "method": "explain",
  "protocol_version": 1,
  "params": {
    "manifest_path": "/path/to/manifest.json",
    "metric_names": ["bookings"],
    "group_by_names": ["metric_time"],
    "where_constraints": null,
    "order_by_names": null,
    "limit": null,
    "sql_engine": "DUCKDB"
  }
}
```

- `manifest_path` — path to a `manifest.json` file **or** a YAML semantic
  manifest directory (for development/testing)
- `sql_engine` — one of `DUCKDB`, `BIGQUERY`, `DATABRICKS`, `POSTGRES`,
  `REDSHIFT`, `SNOWFLAKE`, `TRINO`
- All params except `manifest_path` and `sql_engine` are optional

The engine is cached by `(manifest_path, mtime, sql_engine)` and rebuilt only
when the manifest file changes or the engine type changes, so repeated
`explain` calls against the same manifest are cheap.

Response:

```json
{"id": "1", "ok": true, "sql": "SELECT ..."}
```

#### `ping`

Health check. Responds immediately without touching the manifest or engine.

```json
{"id": "2", "method": "ping", "protocol_version": 1}
```

Response:

```json
{"id": "2", "ok": true}
```

#### `shutdown`

Graceful shutdown. The sidecar responds, flushes stdout, then exits 0.

```json
{"id": "3", "method": "shutdown", "protocol_version": 1}
```

Response:

```json
{"id": "3", "ok": true}
```

### Error responses

All errors share a single shape:

```json
{"id": "<id or null>", "ok": false, "error": {"type": "ExceptionClass", "message": "..."}}
```

`id` is `null` when the request itself could not be parsed or validated
(e.g. malformed JSON, or a missing/invalid field). Common `type` values:

| type | cause |
|---|---|
| `InvalidQueryException` | invalid query parameters |
| `UnknownMetricError` | metric name not found in manifest |
| `ValidationError` | request line was not valid JSON, or didn't match the expected shape |
| `ProtocolVersionError` | `protocol_version` field was not `1` |
| `UnknownMethod` | unrecognised method name |

With `--debug`, error responses also include a `"traceback"` field.

The sidecar continues running after any per-request error. Only `shutdown`,
EOF on stdin, or a signal causes it to exit.

## CLI reference

```
mf_entry.py [--manifest-path PATH] [--sql-engine ENGINE] [--debug] [--version]

  --manifest-path PATH   Pre-load manifest before writing the ready message.
                         Eliminates cold-start latency on the first explain call.
  --sql-engine ENGINE    Engine to use for pre-warming (default: DUCKDB).
  --debug                Verbose stderr logging; include tracebacks in error responses.
  --version              Print version and exit.
```

## stdout protection

Any library `print()` call would corrupt the NDJSON framing on the pipe the caller
is reading. `mf_entry.py` saves the real stdout file descriptor before any
library code runs, then replaces `sys.stdout` with `sys.stderr`. All IPC
writes go through the saved descriptor. All non-IPC output (logging, library
chatter) goes to stderr.
