# MetricFlow-RS Rewrite Roadmap

**Goal:** Rewrite MetricFlow's compilation pipeline (semantic manifest + query → SQL) in Rust as a library crate for native integration with Fusion, replacing the current Python subprocess call.

**Design spec:** `docs/superpowers/specs/2026-03-31-metricflow-rust-rewrite-design.md`

---

## Phase Overview

| Phase | Name | Depends On | Produces | Detailed Plan |
|-------|------|-----------|----------|---------------|
| 1-2 | Foundation + Simple Metrics | — | Working pipeline: manifest → simple metric → DuckDB SQL | `2026-04-01-metricflow-rs-phase1-2.md` |
| 3 | Joins & Dimensions | 1-2 | Multi-hop entity joins to reach dimensions on other semantic models | Not yet written |
| 4 | Derived & Cumulative Metrics | 3 | Derived metric expressions, cumulative window functions, time spine joins | Not yet written |
| 5 | Conversion & Offset Metrics | 4 | Conversion funnels, time-offset comparisons | Not yet written |
| 6 | All SQL Dialects | 5 | Snowflake, BigQuery, Redshift, Postgres, Databricks, Trino renderers | Not yet written |
| 7 | Fusion Integration | 6 | Replace subprocess in `compile_node_context.rs`, `From<FusionManifest>` | Not yet written |
| 8 | Optimization & Hardening | 7 | CTE flattening, column pruning, error diagnostics, perf benchmarks | Not yet written |

Each phase gets a detailed implementation plan written just before execution. Later phase plans will be more accurate after learning from earlier phases.

---

## Phase 1-2: Foundation + Simple Metrics

**Status:** Plan written

**What it builds:**
- `mf-core` crate: all shared types (enums, specs, manifest structs)
- `mf-manifest` crate: JSON parsing, SemanticGraph with metric/measure/dimension lookup
- `mf-planning` crate: dataflow DAG construction for simple metrics (single measure, group-by, filters)
- `mf-sql` crate: SQL AST, dataflow→SQL converter, default + DuckDB renderer
- `mf-cli` crate: standalone CLI binary
- Test fixture extraction from Python snapshots

**Scope limitation:** Only handles simple metrics where all requested dimensions are on the same semantic model as the measure. No joins, no derived/cumulative/conversion/offset metrics.

**Validation:** Python MetricFlow snapshot tests used as oracle.

---

## Phase 3: Joins & Dimensions

**What it builds:**
- Join path resolution in `mf-manifest`: given a measure's source model and a requested dimension on a different model, find the chain of entity joins
- petgraph-based semantic graph with entity edges between models
- `JoinToEntity` dataflow node and corresponding SQL JOIN generation
- Multi-hop join support (dimension reachable through 2+ entity links)

**Key complexity:** This is the hardest algorithmic piece. Python MetricFlow has ~44 files for semantic graph handling. The Rust version should be simpler (petgraph handles the graph traversal), but the join selection logic (choosing the shortest/best path when multiple exist) needs careful porting.

**Test focus:** Queries requiring 1, 2, 3+ joins. Compare SQL output against Python snapshots.

---

## Phase 4: Derived & Cumulative Metrics

**What it builds:**
- Derived metrics: plan each input metric independently, combine via `CombineAggregations` node, apply expression
- Cumulative metrics: `JoinToTimeSpine` node, `WindowFunction` node for running aggregations
- Time spine handling: read time spine table from project configuration, generate date series joins
- Multi-metric query support (queries requesting >1 metric)

**Key complexity:** Derived metrics create recursive planning (a derived metric's inputs may themselves be derived). Cumulative metrics require time spine joins which generate significantly more complex SQL.

**Test focus:** Nested derived metrics, cumulative with and without windows, grain_to_date.

---

## Phase 5: Conversion & Offset Metrics

**What it builds:**
- Conversion funnels: two source reads joined on entity within a time window, computing conversion rates
- Offset metrics: time-shifted comparisons (e.g., `revenue` vs `revenue_last_month`)
- `ConversionFunnel` and time-offset dataflow nodes
- Constant property handling for conversion metrics

**Key complexity:** Conversion metrics have the most complex SQL output (multiple CTEs, window functions, conditional joins). Offset metrics require careful time arithmetic that varies by dialect.

**Test focus:** Conversion rate calculations, offset window/grain variations.

---

## Phase 6: All SQL Dialects

**What it builds:**
- `SqlRenderer` implementations for: Snowflake, BigQuery, Redshift, Postgres, Databricks, Trino
- Dialect-specific differences: identifier quoting, DATE_TRUNC argument order, date arithmetic syntax, window function syntax, NULL handling
- Cross-dialect test matrix: same queries rendered for all 7 dialects

**Key complexity:** Each dialect has subtle differences. The Python codebase has ~130 lines per dialect renderer, mostly overriding expression rendering for time functions and aggregations.

**Test focus:** Run the full snapshot test suite for each dialect. Some dialects may require new fixtures extracted from Python.

---

## Phase 7: Fusion Integration

**What it builds:**
- Replace the subprocess call in Fusion's `compile_node_context.rs` (on `b-per/metrics-jinja-function` branch) with a direct `mf_sql::compile_query()` call
- `From<FusionManifest>` trait to convert Fusion's already-parsed manifest into `mf-core` types, avoiding double-parsing
- Wire up the `metrics()` Jinja function to call Rust MetricFlow natively

**Key complexity:** Aligning type systems between Fusion's manifest representation and `mf-core`. The Fusion branch may have evolved by this point, so the integration needs to be done against the latest state.

**Test focus:** Fusion's existing `test_render_sql_with_metrics_function_*` tests should pass with the Rust backend.

---

## Phase 8: Optimization & Hardening

**What it builds:**
- SQL optimization passes: sub-query reduction (flatten to CTEs), column pruning, alias simplification
- Structured error messages with source location context
- Performance benchmarking: compare compilation speed against Python MetricFlow
- Edge case handling: empty results, NULL measures, reserved keyword columns

**Key complexity:** The CTE optimization requires analyzing which subqueries are referenced multiple times and extracting them. This is a significant piece of the Python codebase's optimizer.

**Test focus:** Optimized SQL output matches Python snapshots. Benchmark suite for compilation latency.

---

## Instructions for Executing Agents

When executing any phase:

1. **Read this roadmap first** to understand where the phase fits in the overall project
2. **Read the design spec** (`docs/superpowers/specs/2026-03-31-metricflow-rust-rewrite-design.md`) for architectural decisions
3. **Read the phase's detailed plan** for step-by-step implementation instructions
4. **Do not implement features from later phases** — each phase has a defined scope
5. **Use Python MetricFlow snapshot tests as the oracle** for SQL correctness
6. **The Python MetricFlow codebase** is at `/Users/bper/dev/metricflow` — reference it for understanding behavior, but do not port code line-by-line
7. **The Fusion codebase** is at `/Users/bper/dev/fs/` — the `b-per/metrics-jinja-function` branch has the integration point (note: feature branch, not on main)
