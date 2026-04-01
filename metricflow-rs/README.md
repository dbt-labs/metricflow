# MetricFlow-RS

Rust rewrite of [MetricFlow](https://github.com/dbt-labs/metricflow)'s compilation pipeline. Compiles metric queries into SQL strings from a dbt semantic manifest.

Built for native integration with [Fusion](https://github.com/sdf-labs/sdf) (Rust rewrite of dbt), replacing the current Python subprocess call.

## Status

**Phase 1-4 (Foundation + Simple Metrics + Joins + Derived/Cumulative)** is complete. The pipeline supports:

- Simple metrics (single measure with SUM, COUNT, AVG, etc.)
- Group-by dimensions (categorical and time dimensions on the same semantic model)
- Joins to dimensions on other semantic models (entity-based LEFT OUTER JOIN)
- Derived metrics (arbitrary SQL expressions over input metrics)
- Ratio metrics (numerator / denominator with NULLIF safety)
- Cumulative metrics (windowed, grain-to-date, and all-time via time spine joins)
- DuckDB SQL dialect (other dialects fall back to ANSI SQL)
- ORDER BY and LIMIT

Not yet supported (future phases):

- Conversion and offset metrics (Phase 5)
- All SQL dialects: Snowflake, BigQuery, Redshift, Postgres, Databricks, Trino (Phase 6)
- Fusion integration (Phase 7)
- SQL optimization passes (Phase 8)

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
  --metrics bookings \
  --group-by metric_time \
  --grain day \
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
cargo test --all --lib        # unit tests (59 tests)
cargo test --test integration # end-to-end tests (10 tests)
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
