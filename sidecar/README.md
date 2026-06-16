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
  validate_mf_entry.py   validation helper: compares Python vs binary SQL output
  tests/
    test_mf_entry.py     subprocess integration tests (mf-ipc v1)
```

## Development

Run the integration tests against the Python interpreter (no compilation needed):

```bash
hatch run dev-env:pytest sidecar/tests/ -v
```

Quick smoke test from the repo root:

```bash
MANIFEST=metricflow_semantics/test_helpers/semantic_manifest_yamls/sg_00_minimal_manifest
printf '{"id":"1","method":"explain","v":1,"params":{"manifest_path":"%s","metric_names":["bookings"],"group_by_names":["metric_time"],"sql_engine":"DUCKDB"}}\n{"id":"2","method":"shutdown","v":1}\n' "$MANIFEST" \
  | hatch run dev-env:python sidecar/mf_entry.py
```

## Building

Compile `mf_entry.py` to a standalone binary:

```bash
hatch run nuitka-build:compile
```

Output lands in `sidecar/mf_entry.dist/`. The binary is named `mf_entry.bin`
on macOS/Linux and `mf_entry.exe` on Windows.

## Validation

After building, confirm the binary produces identical SQL to the Python
interpreter:

```bash
hatch run nuitka-build:validate
```

This sends the same `explain` request to both and diffs the `sql` field of
each response. A mismatch means something in the Nuitka build is diverging
from the interpreter.

## mf-ipc v1 protocol

All messages are newline-delimited JSON (NDJSON). The protocol is strictly
sequential: the caller sends one request and waits for one response before
sending the next.

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
{"id": "<string or int>", "method": "<method>", "v": 1, "params": {...}}
```

`id` is echoed back in the response. `v` must be `1`.

### Methods

#### `explain`

Compiles a metric query to SQL without executing it.

```json
{
  "id": "1",
  "method": "explain",
  "v": 1,
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
{"id": "2", "method": "ping", "v": 1}
```

Response:

```json
{"id": "2", "ok": true}
```

#### `shutdown`

Graceful shutdown. The sidecar responds, flushes stdout, then exits 0.

```json
{"id": "3", "method": "shutdown", "v": 1}
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

`id` is `null` when the request itself could not be parsed (e.g. malformed
JSON). Common `type` values:

| type | cause |
|---|---|
| `InvalidQueryException` | invalid query parameters |
| `UnknownMetricError` | metric name not found in manifest |
| `JSONDecodeError` | request line was not valid JSON |
| `ProtocolVersionError` | `v` field was not `1` |
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
