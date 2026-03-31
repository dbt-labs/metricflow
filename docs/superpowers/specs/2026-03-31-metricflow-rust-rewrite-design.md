# MetricFlow Rust Rewrite — Design Spec

## Motivation

MetricFlow is currently a ~950K-line Python codebase that powers dbt's semantic layer. Fusion (the Rust rewrite of dbt) currently integrates with MetricFlow via a subprocess call (`uvx mf query --compile`), which is slow and fragile.

The goal is to rewrite MetricFlow's **compilation pipeline** as a native Rust library crate that Fusion can call directly, while also supporting standalone CLI usage.

## Approach: Layered Extraction (Approach C)

Rather than porting all ~950K lines, we extract and reimplement only the **compilation pipeline**: the path from (semantic manifest + query parameters) to a compiled SQL string. The Python test suite (thousands of snapshot files) serves as the oracle for correctness.

The architecture is designed so the SQL generation layer can later emit DataFusion LogicalPlans (for deeper Fusion integration) without changing the planning layer.

## Crate Structure

```
metricflow-rs/
├── Cargo.toml                    # workspace root
├── crates/
│   ├── mf-core/                  # shared types, specs, enums
│   ├── mf-manifest/              # parse SemanticManifest, build semantic graph
│   ├── mf-planning/              # query spec → dataflow plan (DAG)
│   ├── mf-sql/                   # dataflow plan → SQL AST → rendered SQL string
│   └── mf-cli/                   # standalone CLI binary
```

## Public API

The primary interface exposed to Fusion:

```rust
/// Compile a metrics query into a SQL string.
pub fn compile_query(
    manifest: &SemanticManifest,
    query: &QuerySpec,
    dialect: SqlDialect,
) -> Result<String>
```

### Fusion Integration Point

In `fs/sa/crates/dbt-jinja-utils/src/phases/compile/compile_node_context.rs`, the current subprocess call:

```rust
let output = Command::new("uvx")
    .args(["mf", "query", "--compile", ...])
    .output()?;
```

Becomes:

```rust
let sql = mf_sql::compile_query(&manifest, &query_spec, SqlDialect::DuckDB)?;
```

### Standalone CLI

`mf-cli` wraps the same library for independent use:

```
mf query --metrics revenue,costs --group-by "Dimension('region')" --grain month --dialect duckdb
```

## Crate Details

### `mf-core` — Shared Types

Core vocabulary types used across all crates. Rust enums replace Python's class hierarchies and visitor pattern.

**Metric types:**

```rust
enum MetricType {
    Simple { measure: MeasureRef, filter: Option<WhereFilter> },
    Derived { expr: String, metrics: Vec<MetricRef> },
    Cumulative { measure: MeasureRef, window: Option<TimeWindow>, grain_to_date: Option<TimeGrain> },
    Conversion { base_measure: MeasureRef, conversion_measure: MeasureRef, entity: EntityRef, window: TimeWindow },
    Offset { metric: MetricRef, offset: TimeOffset },
}
```

**Query specification:**

```rust
struct QuerySpec {
    metrics: Vec<MetricRef>,
    group_by: Vec<GroupBySpec>,
    where_clauses: Vec<WhereFilter>,
    order_by: Vec<OrderBySpec>,
    limit: Option<u64>,
}

enum GroupBySpec {
    Dimension { name: String, entity_path: Vec<EntityRef> },
    TimeDimension { name: String, grain: TimeGrain, entity_path: Vec<EntityRef> },
    Entity { name: String, entity_path: Vec<EntityRef> },
}
```

**Enumerations:**

```rust
enum TimeGrain { Day, Week, Month, Quarter, Year }
struct TimeWindow { count: u64, grain: TimeGrain }
enum SqlDialect { BigQuery, Snowflake, Redshift, Postgres, Databricks, DuckDB, Trino }
```

### `mf-manifest` — Manifest Parsing & Semantic Graph

**Input:** `semantic_manifest.json` (the same file Python MetricFlow consumes).

**Output:** A `SemanticGraph` — a petgraph-based DAG of semantic models, metrics, measures, dimensions, entities, and their join relationships.

```rust
struct SemanticManifest {
    semantic_models: Vec<SemanticModel>,
    metrics: Vec<Metric>,
    project_configuration: ProjectConfiguration,
}

struct SemanticModel {
    name: String,
    node_relation: NodeRelation,
    measures: Vec<Measure>,
    dimensions: Vec<Dimension>,
    entities: Vec<Entity>,
    primary_entity: Option<EntityRef>,
}

struct SemanticGraph {
    graph: petgraph::DiGraph<SemanticNode, JoinEdge>,
    metrics_by_name: HashMap<String, MetricIdx>,
    models_by_entity: HashMap<EntityRef, Vec<ModelIdx>>,
    join_paths: JoinPathResolver,
}
```

**Key responsibility:** Join path resolution — given a metric's source semantic model and a requested dimension, find the chain of entity joins needed to reach it.

**Note for future:** Initially parses `semantic_manifest.json` independently. Later, add `From<FusionManifest>` conversion to avoid double-parsing when called from Fusion (which already parses the dbt manifest).

### `mf-planning` — Query Planning

Takes a `QuerySpec` + `SemanticGraph` and produces a `DataflowPlan`.

**Pipeline:**

```
QuerySpec + SemanticGraph
    → Query Resolution (validate & resolve references)
    → Measure Selection (which measures feed which metrics)
    → Join Planning (entity join paths to reach requested dimensions)
    → Dataflow DAG Construction
    → Optimization Passes
    → DataflowPlan
```

**Dataflow nodes as an enum:**

```rust
enum DataflowNode {
    ReadFromSource { semantic_model: SemanticModelRef },
    JoinToEntity { left: NodeId, right: NodeId, join_type: JoinType, on_entity: EntityRef },
    JoinToTimeSpine { input: NodeId, time_spine: TimeSpineRef },
    Aggregate { input: NodeId, group_by: Vec<GroupBySpec>, measures: Vec<MeasureAggregation> },
    ComputeMetric { input: NodeId, metric: MetricRef, expr: Option<String> },
    FilterElements { input: NodeId, include: Vec<ColumnSpec> },
    WhereFilter { input: NodeId, filter: WhereFilter },
    OrderBy { input: NodeId, specs: Vec<OrderBySpec> },
    Limit { input: NodeId, count: u64 },
    CombineAggregations { inputs: Vec<NodeId>, join_on: Vec<GroupBySpec> },
    WindowFunction { input: NodeId, window: CumulativeWindow },
    ConversionFunnel { base: NodeId, conversion: NodeId, entity: EntityRef, window: TimeWindow },
}

struct DataflowPlan {
    dag: petgraph::DiGraph<DataflowNode, ()>,
    sink: NodeIdx,
}
```

**Metric type → plan shape:**

| Metric Type | Plan Shape |
|---|---|
| Simple | ReadFromSource → Aggregate → ComputeMetric |
| Derived | Plan each input metric → CombineAggregations → ComputeMetric(expr) |
| Cumulative | ReadFromSource → JoinToTimeSpine → WindowFunction → Aggregate |
| Conversion | Two ReadFromSource → ConversionFunnel (join with time window) |
| Offset | Plan base metric → JoinToTimeSpine with offset |

**Optimization passes** (functions, not class hierarchy):
1. Column pruning — remove columns not needed downstream
2. Source scan optimization — push filters closer to source reads
3. Predicate pushdown — move WHERE clauses as early as possible

### `mf-sql` — SQL Generation

Two-stage process: DataflowPlan → SqlPlan → rendered SQL string.

**Stage 1: SQL AST**

```rust
enum SqlNode {
    Select {
        columns: Vec<SqlColumn>,
        from: SqlFrom,
        joins: Vec<SqlJoin>,
        where_clause: Option<SqlExpr>,
        group_by: Vec<SqlExpr>,
        order_by: Vec<SqlOrderBy>,
        limit: Option<u64>,
    },
    Cte { name: String, query: Box<SqlNode> },
    CteBlock { ctes: Vec<SqlNode>, final_select: Box<SqlNode> },
}
```

**Stage 2: Dialect-specific rendering**

```rust
trait SqlRenderer {
    fn render_select(&self, node: &SqlNode) -> String;
    fn quote_identifier(&self, name: &str) -> String;
    fn render_time_grain(&self, expr: &str, grain: TimeGrain) -> String;
    fn render_date_add(&self, expr: &str, count: i64, grain: TimeGrain) -> String;
    fn render_window_function(&self, func: &WindowFunc) -> String;
}

// One implementation per dialect
struct BigQueryRenderer;
struct SnowflakeRenderer;
struct PostgresRenderer;
struct RedshiftRenderer;
struct DatabricksRenderer;
struct DuckDbRenderer;
struct TrinoRenderer;
```

**SQL optimization passes** (applied to SqlPlan before rendering):
1. Sub-query reduction — flatten unnecessary nesting into CTEs
2. Column pruning — remove unused SELECT columns
3. Alias simplification

**Future DataFusion path:** The `SqlNode` AST is the seam. An alternative `fn to_logical_plan(&SqlNode) -> LogicalPlan` can be added later without touching the planning layer.

## Testing Strategy

### Oracle: Python Snapshot Tests

The Python codebase has thousands of snapshot files — each is expected SQL for a given query + manifest. We extract these as the ground truth.

**Extraction:** A Python script runs the existing MetricFlow test suite but dumps inputs/outputs as JSON fixtures:

```json
{
  "manifest": { ... },
  "query_spec": { "metrics": ["revenue"], "group_by": ["Dimension('region')"], "grain": "month" },
  "dialect": "duckdb",
  "expected_sql": "SELECT ..."
}
```

**Rust test harness:**

```rust
#[test_case("tests/fixtures/simple_metric_query.json")]
#[test_case("tests/fixtures/derived_metric_with_joins.json")]
fn test_sql_output(fixture_path: &str) {
    let fixture: TestFixture = load_fixture(fixture_path);
    let manifest = mf_manifest::parse(&fixture.manifest)?;
    let graph = mf_manifest::build_graph(&manifest)?;
    let plan = mf_planning::plan_query(&graph, &fixture.query_spec)?;
    let sql = mf_sql::render(&plan, fixture.dialect)?;
    assert_eq!(sql.trim(), fixture.expected_sql.trim());
}
```

### Per-Crate Testing

| Crate | Test Focus |
|---|---|
| `mf-core` | Type construction, validation, serde round-trips |
| `mf-manifest` | Parse real manifests, verify graph structure matches Python |
| `mf-planning` | Dataflow DAG shape snapshots for known queries |
| `mf-sql` | Full pipeline snapshot tests + per-dialect unit tests |

## Implementation Phases

### Phase 1: Foundation (`mf-core` + `mf-manifest`)
- Define all core types
- Parse `semantic_manifest.json` into typed structs
- Build semantic graph with join path resolution
- **Test:** round-trip manifest parsing, verify graph structure against Python

### Phase 2: Simple Metrics (`mf-planning` + `mf-sql`)
- Plan and generate SQL for simple metrics (single measure, aggregation, group by, filters)
- Single dialect first: DuckDB (easiest to test locally)
- **Test:** snapshot comparison against Python output

### Phase 3: Joins & Dimensions
- Entity join path planning (the hardest algorithmic piece)
- Multi-hop joins to reach dimensions from different semantic models
- **Test:** queries requiring 1, 2, 3+ joins

### Phase 4: Derived & Cumulative Metrics
- Derived metric expression evaluation
- Cumulative metrics with time spine joins and window functions
- **Test:** snapshot comparison for complex metric types

### Phase 5: Conversion & Offset Metrics
- Conversion funnel logic
- Time-offset comparisons
- **Test:** snapshot comparison for remaining metric types

### Phase 6: All Dialects
- Implement remaining SqlRenderer implementations (Snowflake, BigQuery, Redshift, Postgres, Databricks, Trino)
- **Test:** same queries across all dialects

### Phase 7: Fusion Integration
- Replace subprocess call in `compile_node_context.rs` with `mf_sql::compile_query()`
- Add `From<FusionManifest>` conversion to avoid double-parsing
- CLI binary for standalone use

### Phase 8: Optimization & Hardening
- SQL optimization passes (CTE flattening, column pruning)
- Error messages with good diagnostics
- Performance benchmarking against Python version

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Enums vs visitor pattern | Enums + pattern matching | Idiomatic Rust, less boilerplate, compiler checks exhaustiveness |
| DAG library | petgraph | Aligns with Fusion's existing usage |
| SQL output | Own SQL AST → string | Start simple, design seam for future DataFusion LogicalPlan output |
| Manifest parsing | Independent parsing | Decoupled from Fusion initially; `From<FusionManifest>` added later |
| Test oracle | Python snapshot extraction | Thousands of validated input/output pairs already exist |
| Dialect handling | Trait with per-dialect structs | Clean separation, easy to add new dialects |
| Error handling | `thiserror` + custom Result type | Aligns with Rust ecosystem conventions |

## Dependencies (Expected)

```toml
[workspace.dependencies]
petgraph = "0.7"          # DAG representation (matches Fusion)
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"
thiserror = "2"
clap = { version = "4", features = ["derive"] }  # CLI
itertools = "0.13"
```

## Out of Scope (For Now)

- **Execution engine** — Fusion handles SQL execution via its adapter system
- **MetricFlow Server** — GraphQL server is not needed; Fusion calls the library directly
- **Python compatibility layer** — no PyO3 bindings planned initially
- **dbt integration CLI** — the `dbt-metricflow` Python package is replaced by Fusion's native `metrics()` function
