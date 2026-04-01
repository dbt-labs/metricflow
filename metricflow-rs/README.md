# MetricFlow-RS

Rust rewrite of [MetricFlow](https://github.com/dbt-labs/metricflow)'s compilation pipeline. Compiles metric queries into SQL strings from a dbt semantic manifest.

Built for native integration with [Fusion](https://github.com/sdf-labs/sdf) (Rust rewrite of dbt), replacing the current Python subprocess call.

## Status

The pipeline compiles **531/531 metrics** from the internal-analytics semantic manifest. Supported features:

- **All metric types:** simple, derived, ratio, cumulative (windowed, grain-to-date, all-time)
- **Multi-metric queries:** multiple metrics in a single query, combined via FULL OUTER JOIN
- **Nested metrics:** derived/ratio metrics whose inputs are themselves derived
- **Cross-model joins:** entity-based LEFT OUTER JOIN for dimensions on other semantic models
- **Metric-level WHERE filters:** template resolution for `{{ Dimension(...) }}`, `{{ TimeDimension(...) }}`, `{{ Entity(...) }}`
- **Time dimensions:** auto-detection from manifest, DATE_TRUNC with configurable grain
- **Dundered group-by parsing:** `metric_time__month`, `listing__country`, `listing__ds__week`
- **ORDER BY and LIMIT**
- **DuckDB SQL dialect** (other dialects fall back to ANSI SQL)

### Known Limitations

- **No query-level WHERE filters** — only metric-level filters are supported; the `where_clauses` field on `QuerySpec` is not yet wired through
- **No `fill_nulls_with`** — metrics with `fill_nulls_with` configured are compiled but the null-filling is not applied
- **No multi-hop joins** — only single-hop entity joins (e.g., `listing__country`), not `listing__user__country`
- **No conversion or offset metrics** — `offset_window`, `offset_to_grain`, conversion metric types
- **Limited dialect support** — Snowflake, BigQuery, Redshift, Postgres, Databricks, Trino all fall back to ANSI SQL

## Crate Structure

| Crate | Purpose |
|---|---|
| `mf-core` | Shared types: manifest structs, query specs, enums, SQL dialect |
| `mf-manifest` | Parse `semantic_manifest.json`, build semantic graph index |
| `mf-planning` | Build dataflow plan DAG from query spec + semantic graph |
| `mf-sql` | Convert dataflow plan to SQL AST, render to dialect-specific SQL string |
| `mf-cli` | Standalone CLI binary |

## Usage

### As a library

```rust
use mf_core::dialect::SqlDialect;
use mf_core::manifest::SemanticManifest;
use mf_core::spec::*;
use mf_core::types::*;

let manifest: SemanticManifest = serde_json::from_str(&manifest_json)?;

let query = QuerySpec {
    metrics: vec!["bookings".into()],
    group_by: vec![GroupBySpec::TimeDimension {
        name: "metric_time".into(),
        grain: TimeGrain::Day,
        entity_path: vec![],
    }],
    where_clauses: vec![],
    order_by: vec![],
    limit: None,
};

let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB)?;
println!("{sql}");
```

### CLI

```bash
cargo run -p mf-cli -- query \
  --manifest path/to/semantic_manifest.json \
  --metrics bookings,instant_bookings \
  --group-by metric_time__day,listing__country \
  --dialect duckdb
```

## Building

```bash
cd metricflow-rs
cargo build --all
```

## Testing

```bash
cd metricflow-rs
cargo test --all            # all tests (101 tests)
cargo clippy --all -- -D warnings
cargo fmt --all -- --check
```

## Design

See `docs/superpowers/specs/2026-03-31-metricflow-rust-rewrite-design.md` for the full design spec and `docs/superpowers/plans/2026-04-01-metricflow-rs-roadmap.md` for the phase roadmap.

### Pipeline

```
semantic_manifest.json + QuerySpec + SqlDialect
    → mf-manifest: parse manifest, build SemanticGraph
    → mf-planning: resolve query, build DataflowPlan (petgraph DAG)
    → mf-sql: convert to SqlSelect AST, render to SQL string
    → String
```
