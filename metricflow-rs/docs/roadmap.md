# MetricFlow-RS Roadmap

## Current State (April 2026)

The pipeline compiles **531/531 metrics** from the internal-analytics semantic manifest. Core compilation is feature-complete for the supported metric types.

### What Works

- All metric types: simple, derived, ratio, cumulative (windowed, grain-to-date, all-time)
- Multi-metric queries (FULL OUTER JOIN combining with NULL dedup re-aggregation)
- Nested derived metrics (derived wrapping derived, derived inputs to ratio)
- Cross-model entity joins (single-hop LEFT OUTER JOIN)
- WHERE filters: metric-level, query-level, per-input-metric, derived metric filter pushdown
- `fill_nulls_with` (in aggregation and in FULL OUTER JOIN re-aggregation)
- Time dimensions with configurable grain
- ORDER BY, LIMIT
- DuckDB dialect (others fall back to ANSI SQL)
- 29 integration tests + 55 Python compatibility tests (44 passing, 11 skipped)

### Test Coverage

| Suite | Count | Status |
|---|---|---|
| Unit tests (cargo test) | 29 | All passing |
| Python compat tests | 55 | 44 pass, 11 skip (known unsupported features) |
| Oracle tests (531 metrics) | 531 | All compile |

## Phase 1: Correctness & Parity (Current)

Remaining gaps vs Python MetricFlow:

- [ ] **Multi-hop joins** — `listing__user__country` (join through intermediate entities)
- [ ] **Conversion metrics** — `conversion_type_params` metric type
- [ ] **Offset metrics** — `offset_window`, `offset_to_grain` on input metrics (disable filter pushdown for offset inputs, per Python)
- [ ] **Time range constraints** — `WHERE metric_time BETWEEN ...` predicate pushdown
- [ ] **Non-additive dimensions** — `non_additive_dimension` on measures (semi-additive aggregation)
- [ ] **`join_to_timespine` for fill_nulls** — fill_nulls_with that requires a time spine join to materialize missing time periods

## Phase 2: SQL Dialect Support

Currently all non-DuckDB dialects fall back to ANSI SQL. Key dialect differences to implement:

- [ ] **Snowflake** — `IDENTIFIER` quoting, `DIV0` function, `DATE_TRUNC` syntax, `::` casting
- [ ] **BigQuery** — backtick quoting, `DATE_TRUNC(col, MONTH)` arg order, `SAFE_DIVIDE`, `TIMESTAMP` vs `DATETIME`
- [ ] **Redshift** — `DATEADD`/`DATEDIFF` syntax, `GETDATE()`, text type differences
- [ ] **Databricks** — similar to Spark SQL, `DATE_TRUNC` syntax
- [ ] **Trino** — `DATE_TRUNC` syntax, `TRY_CAST`, `DOUBLE` type
- [ ] **Postgres** — mostly ANSI-compatible, `::` casting

**See also:** [SQL Generation Research](sql-generation-research.md) for evaluation of DataFusion's unparser as a potential replacement for manual dialect handling.

## Phase 3: SQL Optimization

MetricFlow-specific optimizations (matching Python implementation):

- [ ] **Predicate pushdown** — push WHERE filters to source scans when safe (already partially done for metric filters)
- [ ] **Scan sharing** — when two metrics read from the same model with the same filters, share the scan
- [ ] **Projection pruning** — only project columns that are actually needed downstream
- [ ] **Time range constraint propagation** — narrow time spine ranges based on query constraints

**Architecture decision:** manual optimization rules vs DataFusion's optimizer framework. See [SQL Generation Research](sql-generation-research.md) for full analysis. Summary:

- **Short-term:** implement critical optimizations manually (predicate pushdown, scan sharing)
- **Long-term:** consider lowering DataflowPlan to DataFusion LogicalPlan and using DataFusion's optimizer framework + SQL unparser for both optimization and multi-dialect generation

## Phase 4: Integration

- [ ] **Fusion integration** — native Rust API for Fusion (SDF's dbt rewrite) to call metricflow-rs directly, replacing Python subprocess
- [ ] **Python bindings** — PyO3 bindings so Python MetricFlow can optionally use the Rust compiler
- [ ] **Benchmarks** — compilation speed comparison vs Python MetricFlow

## Architecture

```
semantic_manifest.json + QuerySpec + SqlDialect
    -> mf-manifest: parse manifest, build SemanticGraph
    -> mf-planning: resolve query, build DataflowPlan (petgraph DAG)
    -> mf-sql: convert to SqlSelect AST, render to dialect-specific SQL string
    -> String
```

Future (if DataFusion adopted):
```
semantic_manifest.json + QuerySpec + SqlDialect
    -> mf-manifest: parse manifest, build SemanticGraph
    -> mf-planning: resolve query, build DataflowPlan (petgraph DAG)
    -> mf-sql: lower to DataFusion LogicalPlan
    -> DataFusion optimizer (built-in + custom rules)
    -> DataFusion unparser (dialect-specific SQL)
    -> String
```
