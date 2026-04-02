# SQL Generation & Optimization: Research Notes

Research on alternatives to handrolled SQL generation in metricflow-rs.

## Problem Statement

metricflow-rs currently builds SQL via a custom AST (~5 types: `SqlSelect`, `SqlExpr`, `SqlFrom`, `SqlJoin`, etc.) in `mf-sql/src/convert.rs`. This works but has limitations:

- **Dialect support is manual** — each dialect difference (quoting, function names, date syntax) must be handled case-by-case
- **No optimization framework** — MetricFlow-specific optimizations (predicate pushdown through metric layers, scan sharing, etc.) would need to be implemented from scratch
- **Fragile string building** — easy to generate subtly incorrect SQL

The ideal replacement would provide: **AST building + optimization rules + multi-dialect SQL generation**.

## Options Evaluated

### 1. Apache DataFusion (Recommended for future)

**What it is:** Rust-native query engine with modular crates for SQL parsing, logical planning, optimization, and (crucially) SQL unparsing.

**Key insight:** You can use DataFusion's optimizer + unparser WITHOUT its execution engine. The relevant crates are independent:
- `datafusion-sql` — SQL parser + unparser (LogicalPlan <-> SQL)
- `datafusion-optimizer` — query optimization with pluggable rules
- `datafusion-expr` — LogicalPlan and expression types

**Architecture for metricflow-rs:**
```
MetricFlow DataflowPlan
    -> Lower to DataFusion LogicalPlan (Scan, Filter, Aggregate, Join, Project)
    -> Apply built-in optimizer passes (predicate pushdown, projection pruning, etc.)
    -> Apply custom MetricFlow optimizer rules (metric-specific pushdown, scan sharing)
    -> Unparse to SQL via plan_to_sql() with dialect selection
```

**Dialect support in unparser:**
- Confirmed: PostgreSQL (default), MySQL, DuckDB, SQLite, BigQuery, Snowflake
- Custom dialects can be implemented via the `Dialect` trait
- Handles identifier quoting, function name mapping, type syntax per dialect

**Maturity (as of early 2026):**
- `plan_to_sql` is functional but output is verbose (excessive parenthesization)
- Some edge cases with exotic join types (LEFT ANTI/SEMI)
- Actively developed, improving rapidly
- Used in production by federated query systems (Wren AI, Spice AI)

**Key links:**
- Unparser module docs: https://docs.rs/datafusion-sql/latest/datafusion_sql/unparser/index.html
- `plan_to_sql` function: https://docs.rs/datafusion/latest/datafusion/sql/unparser/fn.plan_to_sql.html
- Example code: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/plan_to_sql.rs
- Building logical plans: https://datafusion.apache.org/library-user-guide/building-logical-plans.html
- Custom optimizer rules: https://datafusion.apache.org/library-user-guide/query-optimizer.html
- Dialect struct (DuckDB): https://docs.rs/datafusion/latest/datafusion/sql/unparser/dialect/struct.DuckDBDialect.html
- GitHub issues tracking unparser maturity:
  - https://github.com/apache/datafusion/issues/10524 (API examples)
  - https://github.com/apache/datafusion/issues/10557 (prettier output)

**Risks:**
- Unparser may not handle all MetricFlow SQL patterns correctly (FULL OUTER JOIN + COALESCE re-aggregation, time spine joins). Needs validation.
- Large dependency tree (though modular crates help)
- Lowering MetricFlow concepts (ComputeMetric, CombineAggregatedOutputs) to relational ops requires a translation layer

**Recommendation:** Best long-term option. Validate by building one complex query (e.g., NPS with 3-way FULL OUTER JOIN, filter pushdown, fill_nulls_with) using DataFusion's LogicalPlan and unparsing to Snowflake SQL.

### 2. `polyglot-sql`

**What it is:** Rust port of Python's SQLGlot — SQL transpiler supporting 33 dialects.

**Strengths:**
- Production-ready, 9k+ test fixtures from SQLGlot
- 33 dialects: PostgreSQL, MySQL, BigQuery, Snowflake, DuckDB, ClickHouse, Redshift, Spark, Trino, Databricks, etc.
- Parse SQL in one dialect, output in another
- Scope analysis, column lineage, AST traversal

**Weaknesses:**
- No optimizer rule framework — solves dialect generation but not optimization
- You'd build SQL strings first, then transpile — doesn't help with AST construction
- Crate: `polyglot-sql` on crates.io
- GitHub: https://github.com/tobilg/polyglot

**When to use:** If you only need multi-dialect SQL output and don't care about optimization rules. Could also complement DataFusion as a final dialect polishing step.

### 3. `sqlparser-rs`

**What it is:** Rust SQL parser used by DataFusion, Polars, and many others.

**Key finding:** Dialect support is for PARSING only, not generation. `to_string()` outputs one generic SQL flavor regardless of target dialect. You'd still need your own rendering layer.

- Docs: https://docs.rs/sqlparser/latest/sqlparser/
- GitHub: https://github.com/apache/datafusion-sqlparser-rs

**Verdict:** Not useful on its own for our needs. Already used internally by DataFusion.

### 4. `sea-query`

Rust query builder. Limited to MySQL/PostgreSQL/SQLite — no warehouse dialect support. Not a fit.

### 5. Substrait

**What it is:** Cross-language IR for relational algebra (protobuf-based). Designed for plan portability between engines (DataFusion, DuckDB, Velox).

**Why not (for now):**
- Represents relational algebra, not SQL — still need Substrait -> SQL per dialect
- MetricFlow concepts have no Substrait equivalents
- Substrait -> SQL path is immature
- Makes more sense if we want engine interop (run metric query on DuckDB locally OR compile to Snowflake SQL)

**When to reconsider:** If we add a local execution mode (run queries against DuckDB/Parquet without a warehouse).

## Decision

**Short-term (now):** Keep the current handrolled approach. It works, matches Python MetricFlow behavior, and is easy to verify. The custom AST is small and the dialect differences are localized.

**Medium-term:** Evaluate DataFusion's unparser by building a spike for a complex query. If it handles our SQL patterns correctly, plan migration of `convert.rs` to use DataFusion LogicalPlan + unparser.

**Long-term vision:**
```
QuerySpec + SemanticManifest
    -> mf-planning: MetricFlow DataflowPlan (metric-aware DAG)
    -> mf-sql: Lower to DataFusion LogicalPlan (relational ops)
    -> DataFusion optimizer: built-in + custom MetricFlow rules
    -> DataFusion unparser: dialect-specific SQL (Snowflake, BigQuery, etc.)
```

This keeps MetricFlow's domain-specific planning separate from generic SQL optimization and generation, using the best tool for each layer.
