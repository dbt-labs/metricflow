# PyO3 vs Nuitka: Embedding Python MetricFlow in dbt-core v2 (Fusion)

**Date:** 2026-06-15
**Status:** Decision analysis — Nuitka sidecar recommended
**Scope:** Integration strategy for calling Python MetricFlow from the Fusion (dbt-core v2) Rust binary

## Background

The Rust-native `crates/dbt-metricflow` crate is explicitly non-viable long-term: two
implementations of the same query compiler will inevitably diverge. The Python MetricFlow
is authoritative. The question is how to call it from Fusion's Rust binary.

Two options were evaluated:

- **PyO3 embedding** — link libpython into Fusion, call MetricFlow in-process via PyO3's embedding API
- **Nuitka sidecar** — compile Python MetricFlow into a Nuitka standalone binary, invoke via Fusion's existing `SidecarClient` subprocess pattern with JSON IPC over stdin/stdout

---

## Recommendation: Nuitka sidecar

**The Nuitka sidecar is the better approach.** The decisive factors are crash isolation,
concurrency scaling, alignment with Fusion's existing `SidecarClient` infrastructure, and
the debuggability advantage for the MetricFlow Python team. PyO3 embedding carries
architectural risks that compound in production and are hard to mitigate without deep
CPython internals expertise.

---

## Approach Comparison

### PyO3 Embedding

Rust calls `pyo3::prepare_freethreaded_python()`, links `libpython`, imports MetricFlow
Python modules in-process, and calls `MetricFlowEngine.explain()` via dynamic dispatch.

**Unique advantages:**
- Zero process boundary — no IPC serialization overhead (~2ms saved)
- In-process data exchange without subprocess lifecycle management

**Critical issues:**
1. **CPython must initialize on Fusion's main thread before Tokio starts.** Calling
   `prepare_freethreaded_python()` from a Tokio worker thread is undefined behavior in
   CPython. This requires architectural changes to Fusion's startup sequence.
2. **pydantic-core ABI conflict.** pydantic-core is a PyO3-compiled Rust extension. If
   its embedded PyO3 version conflicts with Fusion's PyO3 0.27, two PyO3 runtimes share
   one process. This is untested and may break pydantic v2 — the heart of MetricFlow's
   model validation.
3. **GIL serializes all concurrent MetricFlow compilations.** Only one Python compilation
   at a time. Unscalable for any concurrent workload.
4. **A MetricFlow crash kills Fusion.** RecursionError, MemoryError, or a segfault in
   pydantic-core takes down the entire Fusion process. Rust's reliability guarantee does
   not extend to embedded Python.
5. **libpython distribution is fragile.** `libpython3.11.so.1.0` soname differs across
   Debian/RPM/musl Linux. Static linking is non-trivial. macOS uses framework builds
   that require special linker flags. Requires system Python or bundled libpython.
6. **MetricFlow updates require Fusion releases.** Python environment is baked into or
   discovered by the Fusion binary — no independent update path.
7. **Debugging requires running Fusion.** The MetricFlow team cannot test integration
   changes without building the full Fusion binary.

**Probability of clean PoC success: ~50%** (pydantic-core ABI and tokio/main-thread
constraints are unvalidated unknowns).

---

### Nuitka Sidecar

Nuitka compiles Python MetricFlow to a standalone native binary (bundling CPython + all
dependencies). Fusion spawns this binary as a subprocess, communicates over JSON IPC via
stdin/stdout — the same pattern as Fusion's existing `SidecarClient` trait.

**Key advantages:**
1. **Crash isolation.** MetricFlow process failure is detected via EOF on stdout. Fusion
   returns an error, restarts the sidecar, continues serving. Fusion cannot crash due to
   Python.
2. **Extends existing infrastructure.** `SidecarClient` trait already exists in
   `crates/dbt-adapter/src/engine/sidecar_client.rs`. Subprocess delegation is an
   established Fusion pattern.
3. **Hermetic distribution.** Nuitka standalone bundles CPython — no system Python
   required. Completely independent of user Python installations.
4. **True concurrency.** A pool of sidecar processes serves concurrent compilation
   requests in parallel — no GIL contention.
5. **Standalone debuggability.** `echo '{"method":"explain",...}' | ./mf_entry` — the
   MetricFlow team can test their entry point without touching Rust.
6. **Aligns with dbt Cloud SLG pattern.** The Semantic Layer Gateway already runs Python
   MetricFlow as a long-running server. Nuitka sidecar is a hermetic version of the same
   architecture.
7. **Versioned IPC protocol.** Explicit contract between Fusion and MetricFlow; API
   changes are caught at the protocol boundary, not at runtime inside Fusion.
8. **Pure Rust binary.** Fusion's Rust binary has no libpython linkage, no CPython ABI
   sensitivity, no Python-version-specific build constraints.
9. **Sequenceable.** The IPC protocol and Rust subprocess client work with regular Python
   subprocess first; Nuitka compilation is an additive packaging step. Don't block
   architecture work on Nuitka feasibility.

**Operational risks:**
- Nuitka pydantic v2 compatibility with `model_validator`, generic models, and
  `fast_frozen_dataclass` — requires empirical PoC validation.
- Windows Defender false positives on Nuitka-compiled binaries — known issue, requires
  EV code signing or Microsoft malware clearance submission.
- macOS notarization of bundled native extensions — each `.so` requires individual signing
  with dbt Labs Developer ID; automation-heavy but solvable.

**Probability of clean PoC success: ~70-75%** (risks are operational, detectable at build
time, not architectural unknowns).

---

## Architecture

```
Fusion Binary (Rust) [standalone distribution]
├── MetricFlowSidecarClient : SidecarClient
│   ├── on_startup: extract mf_entry.dist/ to $XDG_CACHE_DIR/dbt/metricflow/
│   ├── spawn sidecar process: ./mf_entry [--manifest <path>]
│   ├── JSON IPC over stdin/stdout (NDJSON, one line per request/response)
│   ├── sidecar pool: N processes for concurrent compilation
│   ├── health check: periodic ping, restart on crash
│   └── manifest reload: {"method":"reload","manifest_path":"..."} → {"status":"ready"}
│
├── Bundled artifact: mf_entry.dist.tar.gz [embedded via rust-embed]
│   ├── CPython 3.11 runtime
│   ├── metricflow + metricflow_semantics + metricflow_semantic_interfaces
│   ├── pydantic-core, sqlglot, Jinja2, PyYAML, rapidfuzz, jsonschema
│   └── mf_entry.py compiled to native binary
│
└── IPC protocol (mf-ipc v1)
    ├── handshake: sidecar writes {"status":"ready","metricflow_version":"X","python_version":"3.11.X"}
    ├── request:  {"id":"<uuid>","method":"explain","v":1,"params":{...}}
    ├── request:  {"id":"<uuid>","method":"reload","v":1,"manifest_path":"..."}
    ├── request:  {"id":"<uuid>","method":"shutdown","v":1}
    ├── response: {"id":"<uuid>","ok":true,"sql":"SELECT ..."}
    └── response: {"id":"<uuid>","ok":false,"error":{"type":"InvalidQueryException","message":"..."}}

mf_entry.py (lives in MetricFlow Python repo — owned by MetricFlow team)
├── stdin loop: read NDJSON → dispatch → write NDJSON
├── MetricFlowEngine singleton (lazy init on first manifest load)
├── structured error JSON for all Python exceptions
└── protocol version negotiation

Ownership split:
  metricflow repo  → mf_entry.py (server: calls MetricFlow Python API directly)
  dbt-core repo    → MetricFlowSidecarClient (client: speaks the IPC protocol)
  shared spec      → IPC JSON protocol definition (versioned, owned by both teams)

Rationale: mf_entry.py imports MetricFlow internals directly and must update in
lockstep with MetricFlow API changes. Keeping it in the MetricFlow repo means
MetricFlow API changes and the corresponding entry point update are a single PR.
If it lived in Fusion, every MetricFlow internal refactor would require a cross-repo PR.
The analogy is a language server (e.g. pyright): the server lives in the Python project,
the LSP client lives in the editor — same boundary here.
```

---

## Implementation Phases

| Phase | Weeks | Work |
|-------|-------|------|
| 1 | 1-2 | Write `mf_entry.py`. Validate IPC with regular Python subprocess against MetricFlow snapshot tests. Run parallel PoCs (PyO3 + Nuitka). |
| 2 | 3-4 | Build `MetricFlowSidecarClient` in Fusion using `SidecarClient` trait. Use regular Python — no Nuitka yet. Validate end-to-end. |
| 3 | 5-7 | Nuitka compilation + snapshot test validation. Fix Nuitka/pydantic compatibility issues. Pin Nuitka version in CI. |
| 4 | 8-10 | Artifact bundling in Fusion distribution via `rust-embed`. Implement extraction and cache. |
| 5 | 11-12 | Platform CI matrix: all 6 targets. Windows signing, macOS notarization, load testing. |
| 6 | GA | Ship Fusion with bundled MetricFlow sidecar. |

The key sequencing principle: **IPC protocol and Rust subprocess client work with regular
Python first. Nuitka is an additive packaging step — don't let Nuitka feasibility
uncertainties block Phase 1 and Phase 2.**

---

## Week 1 Actions (Parallel)

All three are independent and take ~1-2 days each:

1. **Write `mf_entry.py`** — 50-100 line stdin/stdout JSON IPC entry point over
   `MetricFlowEngine`. Run against MetricFlow's snapshot test suite with regular Python.
   Validates IPC boundary design with zero Rust or Nuitka work.

2. **PyO3 embedding PoC** — standalone Rust binary: `prepare_freethreaded_python()` →
   set `sys.path` → import MetricFlow → call `explain()` with a test manifest → print SQL.
   If this fails due to pydantic-core ABI or tokio/main-thread issues: PyO3 is eliminated
   empirically.

3. **Nuitka PoC** — `python -m nuitka --standalone mf_entry.py` → validate output against
   Python reference on simple + derived metric. If Nuitka miscompiles: assess severity and
   mitigation options.

---

## IPC Protocol Design Constraints

Required from day one:

- **Request IDs** — for matching async responses in future multi-request designs
- **Protocol version field** (`"v": 1`) — allows sidecar to reject mismatched Fusion versions
- **Structured error types** — Python exception class name in error response (typed errors in Rust)
- **"ready" handshake** — sidecar signals after init; Fusion waits before sending requests
- **Graceful shutdown** — `{"method":"shutdown"}` for clean process exit

Not needed at first: hot-reload, multi-manifest, streaming responses.

---

## Top 10 Critical Issues Found

| # | Issue | Affects |
|---|-------|---------|
| 1 | CPython must init on main thread before Tokio — `prepare_freethreaded_python()` from Tokio worker is UB | PyO3 only |
| 2 | pydantic-core ABI conflict — two PyO3 runtimes in one process, unvalidated | PyO3 only |
| 3 | GIL serializes all concurrent MetricFlow compilations — 1 compilation at a time | PyO3 only |
| 4 | MetricFlow crash kills Fusion process — RecursionError, MemoryError, segfault in pydantic-core | PyO3 only |
| 5 | libpython distribution across Linux distros — soname fragmentation, musl incompatibility | PyO3 only |
| 6 | Nuitka pydantic v2 compatibility unvalidated — `model_validator`, generics, `fast_frozen_dataclass` | Nuitka only |
| 7 | Windows Defender false positives on Nuitka binaries — documented issue, requires EV signing | Nuitka only |
| 8 | macOS notarization of bundled `.so` files — each extension needs individual signing | Nuitka only |
| 9 | Cold-start latency ~300-700ms — both approaches pay this; mitigate with pre-warm on `dbt parse` | Both |
| 10 | MetricFlow API changes undetected until runtime — string-based dispatch (PyO3) or JSON keys (Nuitka) are untyped | Both (PyO3 worse) |

---

## Top 10 Strengths of Nuitka Sidecar

| # | Strength |
|---|----------|
| 1 | Crash isolation — MetricFlow failure doesn't crash Fusion |
| 2 | Extends existing `SidecarClient` infrastructure — no new patterns |
| 3 | Hermetic — bundles CPython, independent of system Python |
| 4 | True concurrency — sidecar pool, no GIL |
| 5 | Standalone debuggable — MetricFlow team tests without building Fusion |
| 6 | Aligns with dbt Cloud SLG production pattern |
| 7 | Versioned IPC protocol — explicit, auditable contract |
| 8 | `importlib.resources` and package data handled automatically by Nuitka |
| 9 | Rust binary stays pure Rust — no libpython linkage |
| 10 | Sequenceable — start with regular subprocess, add Nuitka packaging later |

---

## Honest Bottom Line

PyO3 embedding is the architecturally elegant option that developers prefer in theory —
in-process, no serialization overhead, no subprocess lifecycle management. But it carries
a cluster of practical risks that compound in production: CPython must be initialized on
the main thread before Tokio starts; pydantic-core's PyO3 ABI may conflict with Fusion's;
libpython must be distributed per-platform; the GIL serializes all MetricFlow compilations;
a crash in MetricFlow's Python code takes down Fusion. The Nuitka sidecar is the less
glamorous option that aligns with everything Fusion has already built (SidecarClient,
subprocess delegation), with how MetricFlow runs in production (dbt Cloud SLG pattern),
and with how similar tools solve this problem (LSP servers, language tool sidecars). Its
risks — Nuitka pydantic v2 compatibility, Windows AV, macOS signing — are real but
operational: detectable at build time, isolable per platform, fixable without architectural
changes. Build the IPC protocol and Rust client with regular Python subprocess first, then
add Nuitka packaging second.
