# Phase 4: Derived & Cumulative Metrics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add derived metrics (including ratio), cumulative metrics (with window and grain_to_date), and multi-metric query support to the Rust MetricFlow pipeline.

**Architecture:** Derived metrics recursively plan each input metric, combine via `CombineAggregatedOutputs` (FULL OUTER JOIN on shared dimensions, or CROSS JOIN when no dimensions), then apply the metric expression. Cumulative metrics join the measure source against a time spine table using an inequality range join. Ratio metrics use the same path as derived (numerator + denominator are input metrics combined then divided).

**Tech Stack:** Rust, petgraph, serde_json, existing mf-core/mf-manifest/mf-planning/mf-sql crates.

**Design spec:** `docs/superpowers/specs/2026-03-31-metricflow-rust-rewrite-design.md`
**Roadmap:** `docs/superpowers/plans/2026-04-01-metricflow-rs-roadmap.md`

---

## Context for Implementers

### Current State (After Phase 3)

The pipeline currently supports:
- Simple metrics (single measure, group-by, filters)
- Entity joins to reach dimensions on other semantic models
- DuckDB SQL dialect

Key files:
- `mf-core/src/types.rs` — Enums: `MetricKind` (Simple, Derived, Cumulative, Conversion, Ratio), `AggregationType`, `TimeGrain`
- `mf-core/src/manifest.rs` — `Metric`, `MetricTypeParams` (has `measure`, `numerator`, `denominator`, `metrics`, `expr`, `window`, `grain_to_date`), `MetricInput`, `MetricTimeWindow`
- `mf-core/src/spec.rs` — `QuerySpec` with `metrics: Vec<String>`, `group_by: Vec<GroupBySpec>`
- `mf-manifest/src/graph.rs` — `SemanticGraph` with `find_metric()`, `models_for_measure()`, `agg_time_dimension()`, `find_join_path()`
- `mf-planning/src/dataflow.rs` — `DataflowNode` enum, `DataflowPlan` (petgraph DAG)
- `mf-planning/src/resolve.rs` — `resolve_simple_metric()` returns `ResolvedSimpleMetric`
- `mf-planning/src/builder.rs` — `build_plan()` currently only handles single simple metrics
- `mf-sql/src/convert.rs` — `to_sql()` converts DataflowPlan → SqlSelect AST
- `mf-sql/src/ast.rs` — `SqlExpr`, `SqlFrom`, `SqlJoin`, `SqlSelect`
- `mf-sql/src/render.rs` — `SqlRenderer` trait, `DefaultRenderer`
- `mf-sql/src/lib.rs` — `compile_query()` top-level API

### How Derived Metrics Work (Python Pattern)

1. For each input metric in `type_params.metrics`, recursively build a full plan (ReadFromSource → Aggregate)
2. If there's only one input, pass it through directly
3. If there are multiple inputs, combine them with `CombineAggregatedOutputsNode` (FULL OUTER JOIN on shared group-by dimensions, CROSS JOIN if no dimensions)
4. Apply `ComputeMetricsNode` which evaluates the `expr` (e.g., `"metric_a - metric_b"`) as a SQL expression where metric names are column references

### How Ratio Metrics Work (Python Pattern)

Identical to derived: numerator and denominator are treated as two input metrics, combined, then the expression is `numerator / NULLIF(denominator, 0)`.

### How Cumulative Metrics Work (Python Pattern)

1. Build a normal simple metric plan (ReadFromSource → pre-aggregation)
2. Insert a `JoinOverTimeRangeNode` that joins the source against a **time spine table**
3. The join is: `time_spine.ds >= source.ds - window AND time_spine.ds < source.ds + 1 day` (for windowed cumulative) or `time_spine.ds >= start_of_grain AND time_spine.ds < source.ds + 1 day` (for grain_to_date)
4. The time spine replaces metric_time in the output — each row of the time spine gets the cumulative sum of all measure rows in its window
5. Then aggregate normally

### Test Fixtures Needed

We'll create manifests with derived, ratio, and cumulative metrics. Time spine configuration is needed for cumulative metrics.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `mf-planning/src/dataflow.rs` | Modify | Add `CombineAggregatedOutputs` and `JoinOverTimeRange` node variants |
| `mf-planning/src/resolve.rs` | Modify | Add `resolve_derived_metric()`, `resolve_ratio_metric()`, `resolve_cumulative_metric()` |
| `mf-planning/src/builder.rs` | Modify | Extend `build_plan()` to handle derived, ratio, cumulative metrics and multi-metric queries |
| `mf-sql/src/convert.rs` | Modify | Add conversion for `CombineAggregatedOutputs` and `JoinOverTimeRange` nodes |
| `mf-sql/src/ast.rs` | Modify | Add `SqlExpr::BinaryOp` variant for arithmetic expressions |
| `mf-sql/src/render.rs` | Modify | Render `BinaryOp` expressions |
| `tests/fixtures/derived_manifest.json` | Create | Manifest with derived metric (`bookings_growth = bookings - instant_bookings`) |
| `tests/fixtures/ratio_manifest.json` | Create | Manifest with ratio metric (`instant_booking_rate = instant_bookings / bookings`) |
| `tests/fixtures/cumulative_manifest.json` | Create | Manifest with cumulative metric + time spine config |
| `tests/integration.rs` | Modify | Add integration tests for derived, ratio, cumulative metrics |

---

## Task 1: Add New Dataflow Node Variants

**Files:**
- Modify: `metricflow-rs/crates/mf-planning/src/dataflow.rs`

- [ ] **Step 1: Write failing test for CombineAggregatedOutputs node**

```rust
#[test]
fn test_combine_aggregated_outputs_node() {
    let mut plan = DataflowPlan::new();

    let read1 = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "bookings_source".into(),
        table: "demo.fct_bookings".into(),
    });
    let agg1 = plan.add_node(DataflowNode::Aggregate {
        group_by: vec!["metric_time__day".into()],
        aggregations: vec![MeasureAggregation {
            measure_name: "bookings".into(),
            agg_type: AggregationType::Sum,
            expr: "1".into(),
            alias: "bookings".into(),
        }],
    });
    plan.add_edge(read1, agg1);

    let read2 = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "bookings_source".into(),
        table: "demo.fct_bookings".into(),
    });
    let agg2 = plan.add_node(DataflowNode::Aggregate {
        group_by: vec!["metric_time__day".into()],
        aggregations: vec![MeasureAggregation {
            measure_name: "instant_bookings".into(),
            agg_type: AggregationType::Sum,
            expr: "is_instant".into(),
            alias: "instant_bookings".into(),
        }],
    });
    plan.add_edge(read2, agg2);

    let combine = plan.add_node(DataflowNode::CombineAggregatedOutputs);
    plan.add_edge(agg1, combine);
    plan.add_edge(agg2, combine);
    plan.set_sink(combine);

    assert_eq!(plan.node_count(), 5);
    let parents = plan.parents(combine);
    assert_eq!(parents.len(), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd metricflow-rs && cargo test -p mf-planning test_combine_aggregated_outputs_node 2>&1 | tail -5`
Expected: FAIL — `CombineAggregatedOutputs` variant doesn't exist

- [ ] **Step 3: Add the new node variants to DataflowNode**

Add these variants to the `DataflowNode` enum in `mf-planning/src/dataflow.rs`:

```rust
/// Combine multiple aggregated metric outputs via FULL OUTER JOIN on shared dimensions.
/// Used for derived/ratio metrics and multi-metric queries.
CombineAggregatedOutputs,
/// Join source data against a time spine for cumulative metric computation.
/// The time spine provides the output metric_time values; the source data is
/// joined with an inequality condition covering the cumulative window.
JoinOverTimeRange {
    /// Time spine table (fully qualified name)
    time_spine_table: String,
    /// Column name in the time spine table (e.g. "ds")
    time_spine_column: String,
    /// The grain of the time spine column
    time_spine_grain: TimeGrain,
    /// Cumulative window (e.g., 7 days). None means "all time".
    window: Option<TimeWindow>,
    /// Grain-to-date (e.g., Month means "month to date"). Mutually exclusive with window.
    grain_to_date: Option<TimeGrain>,
    /// The column name in the source that corresponds to metric_time
    metric_time_column: String,
},
```

Also add `use mf_core::types::TimeWindow;` at the top if not already imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd metricflow-rs && cargo test -p mf-planning test_combine_aggregated_outputs_node 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 5: Write failing test for JoinOverTimeRange node**

```rust
#[test]
fn test_join_over_time_range_node() {
    let mut plan = DataflowPlan::new();

    let read = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "bookings_source".into(),
        table: "demo.fct_bookings".into(),
    });
    let join_time = plan.add_node(DataflowNode::JoinOverTimeRange {
        time_spine_table: "mf_time_spine".into(),
        time_spine_column: "ds".into(),
        time_spine_grain: TimeGrain::Day,
        window: Some(TimeWindow { count: 7, grain: TimeGrain::Day }),
        grain_to_date: None,
        metric_time_column: "ds".into(),
    });
    plan.add_edge(read, join_time);
    plan.set_sink(join_time);

    assert_eq!(plan.node_count(), 2);
    match plan.node(join_time) {
        DataflowNode::JoinOverTimeRange { window, grain_to_date, .. } => {
            assert!(window.is_some());
            assert!(grain_to_date.is_none());
        }
        other => panic!("expected JoinOverTimeRange, got {other:?}"),
    }
}
```

- [ ] **Step 6: Run test to verify it passes** (should already pass since variant was added in step 3)

Run: `cd metricflow-rs && cargo test -p mf-planning test_join_over_time_range_node 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 7: Run all existing tests to verify no regressions**

Run: `cd metricflow-rs && cargo test --all --lib 2>&1 | tail -5`
Expected: All tests pass

- [ ] **Step 8: Fix any clippy/fmt issues and commit**

Run: `cd metricflow-rs && cargo clippy --all -- -D warnings 2>&1 | tail -5 && cargo fmt --all -- --check 2>&1 | tail -5`

```bash
cd metricflow-rs
git add crates/mf-planning/src/dataflow.rs
git commit -m "feat(mf-planning): add CombineAggregatedOutputs and JoinOverTimeRange dataflow nodes"
```

---

## Task 2: Add BinaryOp to SQL AST and Render It

**Files:**
- Modify: `metricflow-rs/crates/mf-sql/src/ast.rs`
- Modify: `metricflow-rs/crates/mf-sql/src/render.rs`

- [ ] **Step 1: Write failing test for BinaryOp rendering**

Add to `mf-sql/src/render.rs` tests:

```rust
#[test]
fn test_render_binary_op() {
    let expr = SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::ColumnRef {
            table_alias: "subq_0".into(),
            column_name: "bookings".into(),
        }),
        op: "-".into(),
        right: Box::new(SqlExpr::ColumnRef {
            table_alias: "subq_0".into(),
            column_name: "instant_bookings".into(),
        }),
    };
    let renderer = DefaultRenderer;
    assert_eq!(
        renderer.render_expr(&expr),
        "subq_0.bookings - subq_0.instant_bookings"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd metricflow-rs && cargo test -p mf-sql test_render_binary_op 2>&1 | tail -5`
Expected: FAIL — `BinaryOp` variant doesn't exist

- [ ] **Step 3: Add BinaryOp variant to SqlExpr**

In `mf-sql/src/ast.rs`, add to the `SqlExpr` enum:

```rust
/// A binary operation: `left op right` (e.g., `a - b`, `a / NULLIF(b, 0)`)
BinaryOp {
    left: Box<SqlExpr>,
    op: String,
    right: Box<SqlExpr>,
},
```

- [ ] **Step 4: Add BinaryOp rendering to DefaultRenderer**

In `mf-sql/src/render.rs`, add to the `render_expr` match in `DefaultRenderer`:

```rust
SqlExpr::BinaryOp { left, op, right } => {
    format!("{} {op} {}", self.render_expr(left), self.render_expr(right))
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd metricflow-rs && cargo test -p mf-sql test_render_binary_op 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 6: Write test for NULLIF ratio expression**

```rust
#[test]
fn test_render_ratio_expression() {
    // Ratio: numerator / NULLIF(denominator, 0)
    let expr = SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::ColumnRef {
            table_alias: "subq_0".into(),
            column_name: "instant_bookings".into(),
        }),
        op: "/".into(),
        right: Box::new(SqlExpr::FunctionCall {
            function: "NULLIF".into(),
            args: vec![
                SqlExpr::ColumnRef {
                    table_alias: "subq_0".into(),
                    column_name: "bookings".into(),
                },
                SqlExpr::Literal("0".into()),
            ],
        }),
    };
    let renderer = DefaultRenderer;
    let rendered = renderer.render_expr(&expr);
    assert!(rendered.contains("NULLIF"));
    assert!(rendered.contains("/"));
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `cd metricflow-rs && cargo test -p mf-sql test_render_ratio_expression 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 8: Fix clippy/fmt and commit**

```bash
cd metricflow-rs
cargo fmt --all
cargo clippy --all -- -D warnings
git add crates/mf-sql/src/ast.rs crates/mf-sql/src/render.rs
git commit -m "feat(mf-sql): add BinaryOp variant to SQL AST for derived/ratio metric expressions"
```

---

## Task 3: Create Test Fixtures for Derived and Ratio Metrics

**Files:**
- Create: `metricflow-rs/tests/fixtures/derived_manifest.json`
- Create: `metricflow-rs/tests/fixtures/ratio_manifest.json`

- [ ] **Step 1: Create derived_manifest.json**

This manifest has two simple metrics (`bookings` and `instant_bookings`) on the same model, plus a derived metric `bookings_growth` with `expr: "bookings - instant_bookings"`.

```json
{
  "semantic_models": [
    {
      "name": "bookings_source",
      "node_relation": {
        "alias": "fct_bookings",
        "schema_name": "demo",
        "database": null
      },
      "defaults": {
        "agg_time_dimension": "ds"
      },
      "primary_entity": "booking",
      "entities": [
        {
          "name": "booking",
          "type": "primary",
          "expr": null
        }
      ],
      "measures": [
        {
          "name": "bookings",
          "agg": "sum",
          "expr": "1",
          "agg_time_dimension": "ds"
        },
        {
          "name": "instant_bookings",
          "agg": "sum_boolean",
          "expr": "is_instant",
          "agg_time_dimension": "ds"
        }
      ],
      "dimensions": [
        {
          "name": "ds",
          "type": "time",
          "type_params": {
            "time_granularity": "day"
          },
          "expr": null
        },
        {
          "name": "is_instant",
          "type": "categorical",
          "expr": null
        }
      ]
    }
  ],
  "metrics": [
    {
      "name": "bookings",
      "type": "simple",
      "type_params": {
        "measure": {
          "name": "bookings",
          "join_to_timespine": false
        }
      }
    },
    {
      "name": "instant_bookings",
      "type": "simple",
      "type_params": {
        "measure": {
          "name": "instant_bookings",
          "join_to_timespine": false
        }
      }
    },
    {
      "name": "bookings_growth",
      "type": "derived",
      "type_params": {
        "expr": "bookings - instant_bookings",
        "metrics": [
          {
            "name": "bookings",
            "alias": "bookings"
          },
          {
            "name": "instant_bookings",
            "alias": "instant_bookings"
          }
        ]
      }
    }
  ],
  "project_configuration": {
    "time_spine_table_configurations": [],
    "time_spines": []
  }
}
```

- [ ] **Step 2: Create ratio_manifest.json**

Same model, but with a ratio metric `instant_booking_rate = instant_bookings / bookings`.

```json
{
  "semantic_models": [
    {
      "name": "bookings_source",
      "node_relation": {
        "alias": "fct_bookings",
        "schema_name": "demo",
        "database": null
      },
      "defaults": {
        "agg_time_dimension": "ds"
      },
      "primary_entity": "booking",
      "entities": [
        {
          "name": "booking",
          "type": "primary",
          "expr": null
        }
      ],
      "measures": [
        {
          "name": "bookings",
          "agg": "sum",
          "expr": "1",
          "agg_time_dimension": "ds"
        },
        {
          "name": "instant_bookings",
          "agg": "sum_boolean",
          "expr": "is_instant",
          "agg_time_dimension": "ds"
        }
      ],
      "dimensions": [
        {
          "name": "ds",
          "type": "time",
          "type_params": {
            "time_granularity": "day"
          },
          "expr": null
        },
        {
          "name": "is_instant",
          "type": "categorical",
          "expr": null
        }
      ]
    }
  ],
  "metrics": [
    {
      "name": "bookings",
      "type": "simple",
      "type_params": {
        "measure": {
          "name": "bookings",
          "join_to_timespine": false
        }
      }
    },
    {
      "name": "instant_bookings",
      "type": "simple",
      "type_params": {
        "measure": {
          "name": "instant_bookings",
          "join_to_timespine": false
        }
      }
    },
    {
      "name": "instant_booking_rate",
      "type": "ratio",
      "type_params": {
        "numerator": {
          "name": "instant_bookings",
          "alias": "instant_bookings"
        },
        "denominator": {
          "name": "bookings",
          "alias": "bookings"
        }
      }
    }
  ],
  "project_configuration": {
    "time_spine_table_configurations": [],
    "time_spines": []
  }
}
```

- [ ] **Step 3: Verify both fixtures parse correctly**

Add a quick test in `mf-manifest/src/parse.rs` tests (or just run manually):

```bash
cd metricflow-rs && cargo test --all --lib 2>&1 | tail -5
```

Should still pass (existing tests unaffected; new fixtures aren't loaded by any test yet).

- [ ] **Step 4: Commit fixtures**

```bash
cd metricflow-rs
git add tests/fixtures/derived_manifest.json tests/fixtures/ratio_manifest.json
git commit -m "feat: add test fixtures for derived and ratio metrics"
```

---

## Task 4: Extend resolve.rs for Derived and Ratio Metrics

**Files:**
- Modify: `metricflow-rs/crates/mf-planning/src/resolve.rs`

- [ ] **Step 1: Write failing test for resolve_derived_metric**

Add to `resolve.rs` (note: you'll need a test that uses the derived_manifest fixture):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mf_manifest::parse;

    // ... existing tests ...

    #[test]
    fn test_resolve_derived_metric() {
        let json = include_str!("../../../tests/fixtures/derived_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_derived_metric(&graph, "bookings_growth").unwrap();
        assert_eq!(resolved.metric.name, "bookings_growth");
        assert_eq!(resolved.expr, "bookings - instant_bookings");
        assert_eq!(resolved.input_metric_names.len(), 2);
        assert!(resolved.input_metric_names.contains(&"bookings".to_string()));
        assert!(resolved.input_metric_names.contains(&"instant_bookings".to_string()));
    }

    #[test]
    fn test_resolve_ratio_metric() {
        let json = include_str!("../../../tests/fixtures/ratio_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_ratio_metric(&graph, "instant_booking_rate").unwrap();
        assert_eq!(resolved.metric.name, "instant_booking_rate");
        assert_eq!(resolved.numerator_name, "instant_bookings");
        assert_eq!(resolved.denominator_name, "bookings");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd metricflow-rs && cargo test -p mf-planning test_resolve_derived_metric 2>&1 | tail -5`
Expected: FAIL — `resolve_derived_metric` doesn't exist

- [ ] **Step 3: Implement resolve_derived_metric and resolve_ratio_metric**

Add to `resolve.rs`:

```rust
/// Resolved information for a derived metric.
#[derive(Debug)]
pub struct ResolvedDerivedMetric<'a> {
    pub metric: &'a Metric,
    /// The SQL expression to evaluate (e.g., "bookings - instant_bookings")
    pub expr: String,
    /// Names of input metrics (in order from type_params.metrics)
    pub input_metric_names: Vec<String>,
    /// Aliases for input metrics (used as column names in the combined output).
    /// Falls back to the metric name if no alias is set.
    pub input_aliases: Vec<String>,
}

/// Resolve a derived metric: find its expression and input metrics.
pub fn resolve_derived_metric<'a>(
    graph: &'a SemanticGraph<'a>,
    metric_name: &str,
) -> Result<ResolvedDerivedMetric<'a>, ResolveError> {
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| ResolveError::UnknownMetric(metric_name.into()))?;

    if metric.metric_type != MetricKind::Derived {
        return Err(ResolveError::NotDerivedMetric(metric_name.into()));
    }

    let expr = metric
        .type_params
        .expr
        .as_ref()
        .ok_or_else(|| ResolveError::NoExpression(metric_name.into()))?
        .clone();

    let input_metrics = metric
        .type_params
        .metrics
        .as_ref()
        .ok_or_else(|| ResolveError::NoInputMetrics(metric_name.into()))?;

    let input_metric_names: Vec<String> = input_metrics.iter().map(|m| m.name.clone()).collect();
    let input_aliases: Vec<String> = input_metrics
        .iter()
        .map(|m| m.alias.as_deref().unwrap_or(&m.name).to_string())
        .collect();

    Ok(ResolvedDerivedMetric {
        metric,
        expr,
        input_metric_names,
        input_aliases,
    })
}

/// Resolved information for a ratio metric.
#[derive(Debug)]
pub struct ResolvedRatioMetric<'a> {
    pub metric: &'a Metric,
    pub numerator_name: String,
    pub numerator_alias: String,
    pub denominator_name: String,
    pub denominator_alias: String,
}

/// Resolve a ratio metric: find its numerator and denominator.
pub fn resolve_ratio_metric<'a>(
    graph: &'a SemanticGraph<'a>,
    metric_name: &str,
) -> Result<ResolvedRatioMetric<'a>, ResolveError> {
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| ResolveError::UnknownMetric(metric_name.into()))?;

    if metric.metric_type != MetricKind::Ratio {
        return Err(ResolveError::NotRatioMetric(metric_name.into()));
    }

    let numerator = metric
        .type_params
        .numerator
        .as_ref()
        .ok_or_else(|| ResolveError::NoNumerator(metric_name.into()))?;

    let denominator = metric
        .type_params
        .denominator
        .as_ref()
        .ok_or_else(|| ResolveError::NoDenominator(metric_name.into()))?;

    Ok(ResolvedRatioMetric {
        metric,
        numerator_name: numerator.name.clone(),
        numerator_alias: numerator.alias.as_deref().unwrap_or(&numerator.name).to_string(),
        denominator_name: denominator.name.clone(),
        denominator_alias: denominator.alias.as_deref().unwrap_or(&denominator.name).to_string(),
    })
}
```

Also add new error variants to `ResolveError`:

```rust
#[error("metric '{0}' is not a derived metric")]
NotDerivedMetric(String),
#[error("metric '{0}' is not a ratio metric")]
NotRatioMetric(String),
#[error("metric '{0}' is not a cumulative metric")]
NotCumulativeMetric(String),
#[error("derived metric '{0}' has no expression")]
NoExpression(String),
#[error("derived metric '{0}' has no input metrics")]
NoInputMetrics(String),
#[error("ratio metric '{0}' has no numerator")]
NoNumerator(String),
#[error("ratio metric '{0}' has no denominator")]
NoDenominator(String),
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-planning test_resolve_derived_metric test_resolve_ratio_metric 2>&1 | tail -10`
Expected: PASS

- [ ] **Step 5: Run all tests, fix clippy/fmt, commit**

```bash
cd metricflow-rs
cargo test --all --lib
cargo fmt --all
cargo clippy --all -- -D warnings
git add crates/mf-planning/src/resolve.rs
git commit -m "feat(mf-planning): add resolve_derived_metric and resolve_ratio_metric"
```

---

## Task 5: Extend builder.rs for Derived and Ratio Metrics

**Files:**
- Modify: `metricflow-rs/crates/mf-planning/src/builder.rs`

This is the core task. The builder needs to:
1. Detect the metric type
2. For derived/ratio: recursively plan each input metric, then combine with `CombineAggregatedOutputs`
3. For each input metric, produce a full subplan (ReadFromSource → optional joins → Aggregate)

- [ ] **Step 1: Write failing test for derived metric plan**

Add to `builder.rs` tests:

```rust
#[test]
fn test_build_derived_metric_plan() {
    let json = include_str!("../../../tests/fixtures/derived_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings_growth".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let plan = build_plan(&graph, &query).unwrap();
    assert!(plan.sink().is_some());

    // The sink should be a ComputeMetric node
    let sink = plan.sink().unwrap();
    match plan.node(sink) {
        DataflowNode::ComputeMetric { metric_name, expr } => {
            assert_eq!(metric_name, "bookings_growth");
            assert_eq!(expr.as_deref(), Some("bookings - instant_bookings"));
        }
        other => panic!("expected ComputeMetric, got {other:?}"),
    }

    // Parent of ComputeMetric should be CombineAggregatedOutputs
    let parents = plan.parents(sink);
    assert_eq!(parents.len(), 1);
    match plan.node(parents[0]) {
        DataflowNode::CombineAggregatedOutputs => {}
        other => panic!("expected CombineAggregatedOutputs, got {other:?}"),
    }

    // CombineAggregatedOutputs should have 2 parents (one per input metric)
    let combine_parents = plan.parents(parents[0]);
    assert_eq!(combine_parents.len(), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd metricflow-rs && cargo test -p mf-planning test_build_derived_metric_plan 2>&1 | tail -5`
Expected: FAIL — builder doesn't handle derived metrics yet

- [ ] **Step 3: Implement derived/ratio metric planning in builder.rs**

Refactor `build_plan()` to dispatch based on metric type. The key changes:

```rust
use crate::resolve::{self, ResolveError, resolve_derived_metric, resolve_ratio_metric};

pub fn build_plan(graph: &SemanticGraph, query: &QuerySpec) -> Result<DataflowPlan, PlanError> {
    if query.metrics.len() != 1 {
        return Err(PlanError::UnsupportedMetricType); // Multi-metric: Task 7
    }

    let metric_name = &query.metrics[0];
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| PlanError::Resolve(ResolveError::UnknownMetric(metric_name.clone())))?;

    match metric.metric_type {
        MetricKind::Simple => build_simple_metric_plan(graph, query, metric_name),
        MetricKind::Derived => build_derived_metric_plan(graph, query, metric_name),
        MetricKind::Ratio => build_ratio_metric_plan(graph, query, metric_name),
        MetricKind::Cumulative => Err(PlanError::UnsupportedMetricType), // Task 6
        MetricKind::Conversion => Err(PlanError::UnsupportedMetricType), // Phase 5
    }
}
```

Extract the existing simple metric logic into `build_simple_metric_plan()` (same code as current `build_plan`).

Add `build_derived_metric_plan()`:

```rust
fn build_derived_metric_plan(
    graph: &SemanticGraph,
    query: &QuerySpec,
    metric_name: &str,
) -> Result<DataflowPlan, PlanError> {
    let resolved = resolve_derived_metric(graph, metric_name)?;
    let mut plan = DataflowPlan::new();

    // Build a sub-plan for each input metric
    let mut input_agg_nodes = Vec::new();
    for (i, input_name) in resolved.input_metric_names.iter().enumerate() {
        let alias = &resolved.input_aliases[i];
        let agg_node = build_input_metric_subplan(
            graph, &query.group_by, input_name, alias, &mut plan,
        )?;
        input_agg_nodes.push(agg_node);
    }

    // Combine: if only 1 input, skip CombineAggregatedOutputs
    let combined = if input_agg_nodes.len() == 1 {
        input_agg_nodes[0]
    } else {
        let combine_node = plan.add_node(DataflowNode::CombineAggregatedOutputs);
        for &agg_node in &input_agg_nodes {
            plan.add_edge(agg_node, combine_node);
        }
        combine_node
    };

    // ComputeMetric applies the expression
    let compute = plan.add_node(DataflowNode::ComputeMetric {
        metric_name: metric_name.to_string(),
        expr: Some(resolved.expr.clone()),
    });
    plan.add_edge(combined, compute);

    // Optional ORDER BY / LIMIT
    let current = add_order_by_and_limit(&mut plan, compute, query);
    plan.set_sink(current);
    Ok(plan)
}
```

Add `build_ratio_metric_plan()`:

```rust
fn build_ratio_metric_plan(
    graph: &SemanticGraph,
    query: &QuerySpec,
    metric_name: &str,
) -> Result<DataflowPlan, PlanError> {
    let resolved = resolve_ratio_metric(graph, metric_name)?;
    let mut plan = DataflowPlan::new();

    let num_node = build_input_metric_subplan(
        graph, &query.group_by, &resolved.numerator_name,
        &resolved.numerator_alias, &mut plan,
    )?;
    let den_node = build_input_metric_subplan(
        graph, &query.group_by, &resolved.denominator_name,
        &resolved.denominator_alias, &mut plan,
    )?;

    let combine_node = plan.add_node(DataflowNode::CombineAggregatedOutputs);
    plan.add_edge(num_node, combine_node);
    plan.add_edge(den_node, combine_node);

    // ComputeMetric with ratio expression: numerator_alias / NULLIF(denominator_alias, 0)
    let expr = format!(
        "CAST({} AS DOUBLE) / CAST(NULLIF({}, 0) AS DOUBLE)",
        resolved.numerator_alias, resolved.denominator_alias
    );
    let compute = plan.add_node(DataflowNode::ComputeMetric {
        metric_name: metric_name.to_string(),
        expr: Some(expr),
    });
    plan.add_edge(combine_node, compute);

    let current = add_order_by_and_limit(&mut plan, compute, query);
    plan.set_sink(current);
    Ok(plan)
}
```

Add the shared `build_input_metric_subplan()` helper:

```rust
/// Build a sub-plan for a single input metric (used as input to derived/ratio metrics).
/// Returns the Aggregate node index.
fn build_input_metric_subplan(
    graph: &SemanticGraph,
    group_by: &[GroupBySpec],
    input_metric_name: &str,
    output_alias: &str,
    plan: &mut DataflowPlan,
) -> Result<NodeIndex, PlanError> {
    // Resolve the input metric (must be simple for now)
    let resolved = resolve::resolve_simple_metric(graph, input_metric_name)?;
    let left_model_name = resolved.model.name.as_str();

    let read_node = plan.add_node(DataflowNode::ReadFromSource {
        model_name: resolved.model.name.clone(),
        table: resolved.model.node_relation.fully_qualified(),
    });

    // Handle joins for cross-model dimensions (same logic as simple metric)
    let mut join_nodes: std::collections::HashMap<String, NodeIndex> =
        std::collections::HashMap::new();

    for group in group_by {
        let dim_name = match group {
            GroupBySpec::Dimension { name, .. } => name.as_str(),
            GroupBySpec::TimeDimension { name, .. } => {
                if name == "metric_time" {
                    continue;
                }
                name.as_str()
            }
            GroupBySpec::Entity { .. } => continue,
        };

        if graph.find_dimension(dim_name, left_model_name).is_some() {
            continue;
        }

        match graph.find_join_path(left_model_name, dim_name) {
            Some(join) => {
                let entity_name = join.entity_name.to_string();
                if join_nodes.contains_key(&entity_name) {
                    continue;
                }
                let right_read = plan.add_node(DataflowNode::ReadFromSource {
                    model_name: join.right_model.name.clone(),
                    table: join.right_model.node_relation.fully_qualified(),
                });
                let join_node = plan.add_node(DataflowNode::JoinOnEntities {
                    entity_name: entity_name.clone(),
                    left_key: join.left_expr.to_string(),
                    right_key: join.right_expr.to_string(),
                    right_model_name: join.right_model.name.clone(),
                });
                plan.add_edge(read_node, join_node);
                plan.add_edge(right_read, join_node);
                join_nodes.insert(entity_name, join_node);
            }
            None => {
                return Err(PlanError::DimensionNotFound(
                    dim_name.into(),
                    left_model_name.into(),
                ));
            }
        }
    }

    let group_by_columns: Vec<String> = group_by.iter().map(|g| g.column_name()).collect();
    let aggregations = vec![MeasureAggregation {
        measure_name: resolved.measure.name.clone(),
        agg_type: resolved.measure.agg,
        expr: resolved.measure.sql_expr().to_string(),
        alias: output_alias.to_string(),
    }];

    let agg_input = if join_nodes.is_empty() {
        read_node
    } else {
        *join_nodes.values().next().unwrap()
    };

    let agg_node = plan.add_node(DataflowNode::Aggregate {
        group_by: group_by_columns,
        aggregations,
    });
    plan.add_edge(agg_input, agg_node);

    Ok(agg_node)
}
```

Also extract the ORDER BY / LIMIT logic into a helper:

```rust
fn add_order_by_and_limit(
    plan: &mut DataflowPlan,
    mut current: NodeIndex,
    query: &QuerySpec,
) -> NodeIndex {
    if !query.order_by.is_empty() {
        let order_specs: Vec<(String, bool)> = query
            .order_by
            .iter()
            .map(|o| (o.column.column_name(), o.descending))
            .collect();
        let order_node = plan.add_node(DataflowNode::OrderBy { specs: order_specs });
        plan.add_edge(current, order_node);
        current = order_node;
    }
    if let Some(count) = query.limit {
        let limit_node = plan.add_node(DataflowNode::Limit { count });
        plan.add_edge(current, limit_node);
        current = limit_node;
    }
    current
}
```

Refactor `build_simple_metric_plan` to use `build_input_metric_subplan` and `add_order_by_and_limit` too, to avoid code duplication.

- [ ] **Step 4: Run tests to verify derived metric plan passes**

Run: `cd metricflow-rs && cargo test -p mf-planning test_build_derived_metric_plan 2>&1 | tail -10`
Expected: PASS

- [ ] **Step 5: Write and run test for ratio metric plan**

```rust
#[test]
fn test_build_ratio_metric_plan() {
    let json = include_str!("../../../tests/fixtures/ratio_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

    let query = QuerySpec {
        metrics: vec!["instant_booking_rate".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let plan = build_plan(&graph, &query).unwrap();
    let sink = plan.sink().unwrap();
    match plan.node(sink) {
        DataflowNode::ComputeMetric { metric_name, expr } => {
            assert_eq!(metric_name, "instant_booking_rate");
            assert!(expr.as_ref().unwrap().contains("NULLIF"));
        }
        other => panic!("expected ComputeMetric, got {other:?}"),
    }
}
```

Run: `cd metricflow-rs && cargo test -p mf-planning test_build_ratio_metric_plan 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 6: Run all existing tests, fix clippy/fmt, commit**

```bash
cd metricflow-rs
cargo test --all --lib
cargo fmt --all
cargo clippy --all -- -D warnings
git add crates/mf-planning/src/builder.rs
git commit -m "feat(mf-planning): support derived and ratio metric plan building"
```

---

## Task 6: Extend convert.rs for CombineAggregatedOutputs and ComputeMetric

**Files:**
- Modify: `metricflow-rs/crates/mf-sql/src/convert.rs`

The converter needs to handle:
- `CombineAggregatedOutputs`: FULL OUTER JOIN the parent subqueries on shared group-by columns; CROSS JOIN if no group-by columns
- `ComputeMetric`: wrap parent in subquery, add metric expression as a SELECT column

- [ ] **Step 1: Write failing test for CombineAggregatedOutputs conversion**

```rust
#[test]
fn test_convert_combine_aggregated_outputs() {
    use mf_core::types::AggregationType;

    let mut plan = DataflowPlan::new();

    // Two independent aggregations
    let read1 = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "bookings_source".into(),
        table: "demo.fct_bookings".into(),
    });
    let agg1 = plan.add_node(DataflowNode::Aggregate {
        group_by: vec!["metric_time__day".into()],
        aggregations: vec![MeasureAggregation {
            measure_name: "bookings".into(),
            agg_type: AggregationType::Sum,
            expr: "1".into(),
            alias: "bookings".into(),
        }],
    });
    plan.add_edge(read1, agg1);

    let read2 = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "bookings_source".into(),
        table: "demo.fct_bookings".into(),
    });
    let agg2 = plan.add_node(DataflowNode::Aggregate {
        group_by: vec!["metric_time__day".into()],
        aggregations: vec![MeasureAggregation {
            measure_name: "instant_bookings".into(),
            agg_type: AggregationType::Sum,
            expr: "is_instant".into(),
            alias: "instant_bookings".into(),
        }],
    });
    plan.add_edge(read2, agg2);

    let combine = plan.add_node(DataflowNode::CombineAggregatedOutputs);
    plan.add_edge(agg1, combine);
    plan.add_edge(agg2, combine);
    plan.set_sink(combine);

    let sql = to_sql_standalone(&plan).unwrap();
    // The outer query should be a SELECT * from a subquery that does a FULL OUTER JOIN
    // of the two aggregation subqueries.
    match &sql.from {
        SqlFrom::Subquery { .. } => {}
        SqlFrom::Table { .. } => panic!("expected subquery"),
    }
    assert!(!sql.joins.is_empty() || matches!(&sql.from, SqlFrom::Subquery { .. }));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd metricflow-rs && cargo test -p mf-sql test_convert_combine_aggregated_outputs 2>&1 | tail -5`
Expected: FAIL — `CombineAggregatedOutputs` not handled in convert_node

- [ ] **Step 3: Implement CombineAggregatedOutputs conversion**

In `convert.rs`, add to the `convert_node` match:

```rust
DataflowNode::CombineAggregatedOutputs => {
    convert_combine_aggregated_outputs(plan, node_idx, subquery_counter, graph)
}
```

And implement:

```rust
fn convert_combine_aggregated_outputs<'a>(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    subquery_counter: &mut u32,
    graph: Option<&'a SemanticGraph<'a>>,
) -> Result<SqlSelect, ConvertError> {
    let parents = plan.parents(node_idx);
    if parents.is_empty() {
        return Err(ConvertError::UnexpectedNode(
            "CombineAggregatedOutputs has no parents".into(),
        ));
    }

    // Convert each parent to SQL
    let mut parent_sqls: Vec<(SqlSelect, String)> = Vec::new();
    for &p in &parents {
        let sql = convert_node(plan, p, subquery_counter, graph)?;
        let alias = format!("subq_{subquery_counter}");
        *subquery_counter += 1;
        parent_sqls.push((sql, alias));
    }

    if parent_sqls.len() == 1 {
        let (sql, _alias) = parent_sqls.into_iter().next().unwrap();
        return Ok(sql);
    }

    // First parent becomes the FROM, rest become FULL OUTER JOINs.
    let (first_sql, first_alias) = parent_sqls.remove(0);

    // Extract group-by columns from first parent for the ON clause
    let group_by_cols: Vec<String> = first_sql
        .group_by
        .iter()
        .filter_map(|expr| match expr {
            SqlExpr::ColumnRef { column_name, .. } => Some(column_name.clone()),
            _ => None,
        })
        .collect();

    let from = SqlFrom::Subquery {
        query: Box::new(first_sql),
        alias: first_alias.clone(),
    };

    let mut joins = Vec::new();
    let mut all_aliases = vec![first_alias.clone()];

    for (parent_sql, alias) in parent_sqls {
        let join_type = if group_by_cols.is_empty() {
            "CROSS JOIN".to_string()
        } else {
            "FULL OUTER JOIN".to_string()
        };

        // Build ON clause: COALESCE(prev_aliases...) = new_alias for each group-by col.
        // For the first join (2 parents total), it's just first.col = second.col.
        // For subsequent joins, use COALESCE of all previous aliases.
        let on_expr = if group_by_cols.is_empty() {
            SqlExpr::Literal("1 = 1".into())
        } else {
            let conditions: Vec<String> = group_by_cols
                .iter()
                .map(|col| {
                    if all_aliases.len() == 1 {
                        format!("{}.{col} = {}.{col}", all_aliases[0], alias)
                    } else {
                        let coalesce_parts: Vec<String> =
                            all_aliases.iter().map(|a| format!("{a}.{col}")).collect();
                        format!(
                            "COALESCE({}) = {}.{col}",
                            coalesce_parts.join(", "),
                            alias
                        )
                    }
                })
                .collect();
            SqlExpr::Literal(conditions.join(" AND "))
        };

        joins.push(SqlJoin {
            join_type,
            source: SqlFrom::Subquery {
                query: Box::new(parent_sql),
                alias: alias.clone(),
            },
            on: on_expr,
        });
        all_aliases.push(alias);
    }

    // Build SELECT columns: COALESCE group-by columns + metric columns from each parent
    let mut select_columns: Vec<SqlExpr> = Vec::new();

    // Group-by columns: COALESCE across all aliases
    for col in &group_by_cols {
        if all_aliases.len() == 1 {
            select_columns.push(SqlExpr::Alias {
                expr: Box::new(SqlExpr::ColumnRef {
                    table_alias: all_aliases[0].clone(),
                    column_name: col.clone(),
                }),
                alias: col.clone(),
            });
        } else {
            let coalesce_parts: Vec<String> =
                all_aliases.iter().map(|a| format!("{a}.{col}")).collect();
            select_columns.push(SqlExpr::Alias {
                expr: Box::new(SqlExpr::Literal(format!(
                    "COALESCE({})",
                    coalesce_parts.join(", ")
                ))),
                alias: col.clone(),
            });
        }
    }

    // Metric columns: from each parent, select non-group-by columns
    for (i, alias) in all_aliases.iter().enumerate() {
        // The parent subqueries have SELECT columns that include group-by and metric columns.
        // We need the metric columns (the ones that aren't in group_by).
        // Since we've wrapped parents in subqueries, just reference alias.metric_name.
        // We'll use a simple approach: SELECT alias.* won't work with FULL OUTER JOIN.
        // Instead, we reference the metric alias from the Aggregate node's select.
        // The Aggregate outputs: group-by cols + metric aliases.
        // We need just the metric aliases from each parent.
        // For simplicity, project all columns from each alias except group-by columns
        // as alias.column_name.
        // Since we don't have column metadata at this level, we'll use a wildcard approach:
        // The outer ComputeMetric node references metric names directly.
        // We rely on the parent subquery's select columns being named correctly.
        let _ = i; // Each metric column is accessible via alias.metric_name
    }

    // Simpler approach: just SELECT * (the ComputeMetric node above will pick columns)
    // But we need COALESCE for group-by columns, so we'll build:
    // SELECT COALESCE(a.col, b.col) AS col, a.metric1, b.metric2
    // We don't have the metric column names here directly. Use a pass-through approach:
    // Return a SELECT with group-by COALESCE + pass-through of all non-group-by columns.
    // Since metric aliases come from the Aggregate's output, just reference them.
    // Note: This is simplified — we'll iterate parent select_columns to find metric aliases.

    // Actually, the simplest correct approach for the converter is:
    // The FROM + JOINs already make all columns available.
    // Build: SELECT COALESCE(...) AS dim, first.metric1, second.metric2, ...
    // The converter doesn't have direct access to which columns are metrics.
    // But the parent SqlSelect's select_columns contain the Alias entries.
    // We'll just pass select_columns = [COALESCE dims + all aliases.*]
    // Actually let's just do SELECT * and let the wrapping handle it.
    // The ComputeMetric node will wrap this in a subquery and select specific columns.

    let combine_alias = format!("combine_{subquery_counter}");
    *subquery_counter += 1;

    Ok(SqlSelect {
        select_columns: select_columns, // COALESCE group-by cols (metric cols passed through)
        from,
        joins,
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    })
}
```

**Important:** The above is a starting point. The implementer should ensure that metric columns from each parent subquery are also included in the SELECT. Since each Aggregate subquery names its metric column as the metric alias (e.g., `bookings`, `instant_bookings`), add:

```rust
// After COALESCE group-by columns, add metric columns from each parent alias.
// We know each parent's Aggregate names its output metric as the alias.
// The simplest approach: for each parent alias, add all non-group-by select columns.
// Since we're wrapping in FULL OUTER JOIN, each alias's metric columns are distinct.
// Just add alias.* minus the group-by columns for each.
// For now, use alias.metric_alias which the ComputeMetric node references.
```

The exact implementation may vary — the key constraint is that the output of CombineAggregatedOutputs must have:
- Group-by columns (COALESCEd if multiple parents)
- Each metric's aggregated value as a named column

- [ ] **Step 4: Implement ComputeMetric conversion**

Add to the `convert_node` match:

```rust
DataflowNode::ComputeMetric { metric_name, expr } => {
    convert_compute_metric(plan, node_idx, metric_name, expr.as_deref(), subquery_counter, graph)
}
```

```rust
fn convert_compute_metric<'a>(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    metric_name: &str,
    expr: Option<&str>,
    subquery_counter: &mut u32,
    graph: Option<&'a SemanticGraph<'a>>,
) -> Result<SqlSelect, ConvertError> {
    let parents = plan.parents(node_idx);
    let parent_sql = convert_node(plan, parents[0], subquery_counter, graph)?;

    let subq_alias = format!("subq_{subquery_counter}");
    *subquery_counter += 1;

    // Pass through group-by columns from parent + add metric expression
    let mut select_columns: Vec<SqlExpr> = Vec::new();

    // Pass through all group-by columns from parent
    for col in &parent_sql.select_columns {
        match col {
            SqlExpr::Alias { alias, .. } => {
                select_columns.push(SqlExpr::ColumnRef {
                    table_alias: subq_alias.clone(),
                    column_name: alias.clone(),
                });
            }
            SqlExpr::ColumnRef { column_name, .. } => {
                select_columns.push(SqlExpr::ColumnRef {
                    table_alias: subq_alias.clone(),
                    column_name: column_name.clone(),
                });
            }
            _ => {}
        }
    }

    // Add computed metric column
    let metric_expr = if let Some(e) = expr {
        // The expression references metric aliases as column names.
        // They're available in the subquery as column names.
        SqlExpr::Alias {
            expr: Box::new(SqlExpr::Literal(e.to_string())),
            alias: metric_name.to_string(),
        }
    } else {
        // Simple passthrough (for simple metrics that go through ComputeMetric)
        SqlExpr::Alias {
            expr: Box::new(SqlExpr::ColumnRef {
                table_alias: subq_alias.clone(),
                column_name: metric_name.to_string(),
            }),
            alias: metric_name.to_string(),
        }
    };
    select_columns.push(metric_expr);

    Ok(SqlSelect {
        select_columns,
        from: SqlFrom::Subquery {
            query: Box::new(parent_sql),
            alias: subq_alias,
        },
        joins: vec![],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    })
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd metricflow-rs && cargo test -p mf-sql test_convert_combine_aggregated_outputs 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 6: Run all tests, fix clippy/fmt, commit**

```bash
cd metricflow-rs
cargo test --all --lib
cargo fmt --all
cargo clippy --all -- -D warnings
git add crates/mf-sql/src/convert.rs
git commit -m "feat(mf-sql): convert CombineAggregatedOutputs and ComputeMetric nodes to SQL"
```

---

## Task 7: End-to-End Integration Tests for Derived and Ratio Metrics

**Files:**
- Modify: `metricflow-rs/tests/integration.rs`

- [ ] **Step 1: Add derived metric integration test**

```rust
#[test]
fn test_end_to_end_derived_metric() {
    let manifest_json = include_str!("fixtures/derived_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings_growth".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();
    eprintln!("Generated SQL (derived):\n{sql}");

    // Should contain FULL OUTER JOIN (combining two input metrics)
    assert!(
        sql.contains("FULL OUTER JOIN"),
        "should have FULL OUTER JOIN: {sql}"
    );
    // Should contain the metric expression
    assert!(
        sql.contains("bookings - instant_bookings"),
        "should have derived expression: {sql}"
    );
    // Should contain both measure aggregations
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    // Should contain metric_time__day
    assert!(
        sql.contains("metric_time__day"),
        "should have time dimension: {sql}"
    );
}
```

- [ ] **Step 2: Add ratio metric integration test**

```rust
#[test]
fn test_end_to_end_ratio_metric() {
    let manifest_json = include_str!("fixtures/ratio_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["instant_booking_rate".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();
    eprintln!("Generated SQL (ratio):\n{sql}");

    // Should contain FULL OUTER JOIN
    assert!(
        sql.contains("FULL OUTER JOIN"),
        "should have FULL OUTER JOIN: {sql}"
    );
    // Should contain NULLIF for safe division
    assert!(
        sql.contains("NULLIF"),
        "should have NULLIF for safe division: {sql}"
    );
    // Should contain both measure aggregations
    assert!(sql.contains("SUM"), "should have SUM: {sql}");
}
```

- [ ] **Step 3: Run integration tests**

Run: `cd metricflow-rs && cargo test --test integration 2>&1 | tail -15`
Expected: All tests pass (old and new)

- [ ] **Step 4: Commit**

```bash
cd metricflow-rs
git add tests/integration.rs
git commit -m "test: add end-to-end integration tests for derived and ratio metrics"
```

---

## Task 8: Create Cumulative Metric Fixture and Resolve

**Files:**
- Create: `metricflow-rs/tests/fixtures/cumulative_manifest.json`
- Modify: `metricflow-rs/crates/mf-planning/src/resolve.rs`

- [ ] **Step 1: Create cumulative_manifest.json**

This manifest has a cumulative metric with a 7-day window and a time spine configuration.

```json
{
  "semantic_models": [
    {
      "name": "bookings_source",
      "node_relation": {
        "alias": "fct_bookings",
        "schema_name": "demo",
        "database": null
      },
      "defaults": {
        "agg_time_dimension": "ds"
      },
      "primary_entity": "booking",
      "entities": [
        {
          "name": "booking",
          "type": "primary",
          "expr": null
        }
      ],
      "measures": [
        {
          "name": "bookings",
          "agg": "sum",
          "expr": "1",
          "agg_time_dimension": "ds"
        }
      ],
      "dimensions": [
        {
          "name": "ds",
          "type": "time",
          "type_params": {
            "time_granularity": "day"
          },
          "expr": null
        },
        {
          "name": "is_instant",
          "type": "categorical",
          "expr": null
        }
      ]
    }
  ],
  "metrics": [
    {
      "name": "trailing_7d_bookings",
      "type": "cumulative",
      "type_params": {
        "measure": {
          "name": "bookings",
          "join_to_timespine": false
        },
        "window": {
          "count": 7,
          "granularity": "day"
        }
      }
    },
    {
      "name": "bookings_mtd",
      "type": "cumulative",
      "type_params": {
        "measure": {
          "name": "bookings",
          "join_to_timespine": false
        },
        "grain_to_date": "month"
      }
    },
    {
      "name": "bookings_all_time",
      "type": "cumulative",
      "type_params": {
        "measure": {
          "name": "bookings",
          "join_to_timespine": false
        }
      }
    }
  ],
  "project_configuration": {
    "time_spine_table_configurations": [
      {
        "location": "demo.mf_time_spine",
        "column_name": "ds",
        "grain": "day"
      }
    ],
    "time_spines": [
      {
        "node_relation": {
          "alias": "mf_time_spine",
          "schema_name": "demo",
          "database": null
        },
        "primary_column": {
          "name": "ds",
          "time_granularity": "day"
        },
        "custom_granularities": []
      }
    ]
  }
}
```

- [ ] **Step 2: Write failing test for resolve_cumulative_metric**

```rust
#[test]
fn test_resolve_cumulative_metric_with_window() {
    let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

    let resolved = resolve_cumulative_metric(&graph, "trailing_7d_bookings").unwrap();
    assert_eq!(resolved.metric.name, "trailing_7d_bookings");
    assert_eq!(resolved.measure.name, "bookings");
    assert!(resolved.window.is_some());
    let w = resolved.window.as_ref().unwrap();
    assert_eq!(w.count, 7);
    assert_eq!(w.granularity, "day");
    assert!(resolved.grain_to_date.is_none());
}

#[test]
fn test_resolve_cumulative_metric_grain_to_date() {
    let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

    let resolved = resolve_cumulative_metric(&graph, "bookings_mtd").unwrap();
    assert_eq!(resolved.metric.name, "bookings_mtd");
    assert!(resolved.grain_to_date.is_some());
    assert!(resolved.window.is_none());
}
```

- [ ] **Step 3: Implement resolve_cumulative_metric**

```rust
/// Resolved information for a cumulative metric.
#[derive(Debug)]
pub struct ResolvedCumulativeMetric<'a> {
    pub metric: &'a Metric,
    pub measure: &'a Measure,
    pub model: &'a SemanticModel,
    pub agg_time_dimension: Option<&'a Dimension>,
    /// Cumulative window (e.g., 7 days). None means "all time".
    pub window: Option<&'a MetricTimeWindow>,
    /// Grain-to-date. None unless this is a "X to date" cumulative metric.
    pub grain_to_date: Option<TimeGrain>,
}

pub fn resolve_cumulative_metric<'a>(
    graph: &'a SemanticGraph<'a>,
    metric_name: &str,
) -> Result<ResolvedCumulativeMetric<'a>, ResolveError> {
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| ResolveError::UnknownMetric(metric_name.into()))?;

    if metric.metric_type != MetricKind::Cumulative {
        return Err(ResolveError::NotCumulativeMetric(metric_name.into()));
    }

    let measure_ref = metric
        .type_params
        .measure
        .as_ref()
        .ok_or_else(|| ResolveError::NoMeasure(metric_name.into()))?;

    let models = graph.models_for_measure(&measure_ref.name);
    let model = models
        .first()
        .ok_or_else(|| ResolveError::NoModelForMeasure(measure_ref.name.clone()))?;

    let measure = model
        .measures
        .iter()
        .find(|m| m.name == measure_ref.name)
        .ok_or_else(|| ResolveError::NoModelForMeasure(measure_ref.name.clone()))?;

    let agg_time_dimension = graph.agg_time_dimension(&measure_ref.name, &model.name);

    let window = metric.type_params.window.as_ref();
    let grain_to_date = metric.type_params.grain_to_date;

    Ok(ResolvedCumulativeMetric {
        metric,
        measure,
        model,
        agg_time_dimension,
        window,
        grain_to_date,
    })
}
```

- [ ] **Step 4: Run tests, fix clippy/fmt, commit**

```bash
cd metricflow-rs
cargo test --all --lib
cargo fmt --all
cargo clippy --all -- -D warnings
git add tests/fixtures/cumulative_manifest.json crates/mf-planning/src/resolve.rs
git commit -m "feat(mf-planning): add resolve_cumulative_metric and cumulative test fixture"
```

---

## Task 9: Extend builder.rs and convert.rs for Cumulative Metrics

**Files:**
- Modify: `metricflow-rs/crates/mf-planning/src/builder.rs`
- Modify: `metricflow-rs/crates/mf-sql/src/convert.rs`

- [ ] **Step 1: Add SemanticGraph method to find time spine**

The builder needs to look up the time spine from the manifest's `project_configuration`. Add to `mf-manifest/src/graph.rs`:

```rust
/// Find the time spine table configuration for a given grain.
/// Falls back to the finest available grain if exact match not found.
pub fn find_time_spine(&self, grain: TimeGrain) -> Option<TimeSpineInfo<'a>> {
    // Check time_spines first (newer format)
    for ts in &self.manifest.project_configuration.time_spines {
        if ts.primary_column.time_granularity <= grain {
            return Some(TimeSpineInfo {
                table: ts.node_relation.fully_qualified(),
                column: ts.primary_column.name.clone(),
                grain: ts.primary_column.time_granularity,
            });
        }
    }
    // Fall back to time_spine_table_configurations (older format)
    for cfg in &self.manifest.project_configuration.time_spine_table_configurations {
        if cfg.grain <= grain {
            return Some(TimeSpineInfo {
                table: cfg.location.clone(),
                column: cfg.column_name.clone(),
                grain: cfg.grain,
            });
        }
    }
    None
}
```

Add the helper struct:

```rust
#[derive(Debug, Clone)]
pub struct TimeSpineInfo<'a> {
    pub table: String,
    pub column: String,
    pub grain: TimeGrain,
}
```

Note: The lifetime parameter on `TimeSpineInfo` can be removed since all fields are owned.

- [ ] **Step 2: Write failing test for cumulative metric plan building**

Add to `builder.rs` tests:

```rust
#[test]
fn test_build_cumulative_metric_plan() {
    let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

    let query = QuerySpec {
        metrics: vec!["trailing_7d_bookings".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let plan = build_plan(&graph, &query).unwrap();
    assert!(plan.sink().is_some());

    // Should have a JoinOverTimeRange node in the plan
    let mut has_time_range_join = false;
    let sink = plan.sink().unwrap();
    // Walk the plan to find JoinOverTimeRange
    fn walk(plan: &DataflowPlan, idx: NodeIndex, found: &mut bool) {
        if matches!(plan.node(idx), DataflowNode::JoinOverTimeRange { .. }) {
            *found = true;
        }
        for p in plan.parents(idx) {
            walk(plan, p, found);
        }
    }
    walk(&plan, sink, &mut has_time_range_join);
    assert!(has_time_range_join, "plan should contain JoinOverTimeRange");
}
```

- [ ] **Step 3: Implement cumulative metric plan building**

Add `build_cumulative_metric_plan` to `builder.rs`:

```rust
fn build_cumulative_metric_plan(
    graph: &SemanticGraph,
    query: &QuerySpec,
    metric_name: &str,
) -> Result<DataflowPlan, PlanError> {
    let resolved = resolve::resolve_cumulative_metric(graph, metric_name)?;
    let mut plan = DataflowPlan::new();

    // Step 1: ReadFromSource for the measure model
    let read_node = plan.add_node(DataflowNode::ReadFromSource {
        model_name: resolved.model.name.clone(),
        table: resolved.model.node_relation.fully_qualified(),
    });

    // Step 2: Find the time spine
    let agg_time_dim = resolved.agg_time_dimension
        .ok_or_else(|| PlanError::Resolve(
            ResolveError::DimensionNotFound("metric_time".into(), resolved.model.name.clone())
        ))?;
    let grain = agg_time_dim.type_params.as_ref()
        .map(|tp| tp.time_granularity)
        .unwrap_or(TimeGrain::Day);

    let time_spine = graph.find_time_spine(grain)
        .ok_or_else(|| PlanError::NoTimeSpine)?;

    // Step 3: JoinOverTimeRange node
    let metric_time_col = agg_time_dim.sql_expr().to_string();
    let join_time = plan.add_node(DataflowNode::JoinOverTimeRange {
        time_spine_table: time_spine.table,
        time_spine_column: time_spine.column,
        time_spine_grain: time_spine.grain,
        window: resolved.window.map(|w| TimeWindow {
            count: w.count as u64,
            grain: w.granularity.parse::<TimeGrain>().unwrap_or(TimeGrain::Day),
        }),
        grain_to_date: resolved.grain_to_date,
        metric_time_column: metric_time_col,
    });
    plan.add_edge(read_node, join_time);

    // Step 4: Aggregate
    let group_by_columns: Vec<String> = query.group_by.iter().map(|g| g.column_name()).collect();
    let agg_node = plan.add_node(DataflowNode::Aggregate {
        group_by: group_by_columns,
        aggregations: vec![MeasureAggregation {
            measure_name: resolved.measure.name.clone(),
            agg_type: resolved.measure.agg,
            expr: resolved.measure.sql_expr().to_string(),
            alias: resolved.metric.name.clone(),
        }],
    });
    plan.add_edge(join_time, agg_node);

    let current = add_order_by_and_limit(&mut plan, agg_node, query);
    plan.set_sink(current);
    Ok(plan)
}
```

Add `NoTimeSpine` to `PlanError`:

```rust
#[error("no time spine configured for cumulative metric")]
NoTimeSpine,
```

Update the `build_plan` match to include cumulative:

```rust
MetricKind::Cumulative => build_cumulative_metric_plan(graph, query, metric_name),
```

- [ ] **Step 4: Implement JoinOverTimeRange conversion in convert.rs**

Add to the `convert_node` match:

```rust
DataflowNode::JoinOverTimeRange {
    time_spine_table,
    time_spine_column,
    time_spine_grain: _,
    window,
    grain_to_date,
    metric_time_column,
} => convert_join_over_time_range(
    plan, node_idx, time_spine_table, time_spine_column,
    window, grain_to_date, metric_time_column,
    subquery_counter, graph,
),
```

```rust
#[allow(clippy::too_many_arguments)]
fn convert_join_over_time_range<'a>(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    time_spine_table: &str,
    time_spine_column: &str,
    window: &Option<TimeWindow>,
    grain_to_date: &Option<TimeGrain>,
    metric_time_column: &str,
    subquery_counter: &mut u32,
    graph: Option<&'a SemanticGraph<'a>>,
) -> Result<SqlSelect, ConvertError> {
    let parents = plan.parents(node_idx);
    let parent_sql = convert_node(plan, parents[0], subquery_counter, graph)?;

    let source_alias = match &parent_sql.from {
        SqlFrom::Table { alias, .. } => alias.clone(),
        SqlFrom::Subquery { alias, .. } => alias.clone(),
    };

    let ts_alias = format!("time_spine_{subquery_counter}");
    *subquery_counter += 1;

    // Build the ON clause for the inequality join
    // For windowed: ts.ds >= source.metric_time - INTERVAL window AND ts.ds < source.metric_time + INTERVAL '1 day'
    // For grain_to_date: ts.ds >= DATE_TRUNC(grain, source.metric_time) AND ts.ds < source.metric_time + INTERVAL '1 day'
    // For all-time: ts.ds <= source.metric_time (no lower bound)

    let upper_bound = format!(
        "{ts_alias}.{time_spine_column} <= {source_alias}.{metric_time_column}"
    );

    let lower_bound = if let Some(w) = window {
        format!(
            "{ts_alias}.{time_spine_column} > {source_alias}.{metric_time_column} - INTERVAL '{} {}'",
            w.count, w.grain
        )
    } else if let Some(grain) = grain_to_date {
        format!(
            "{ts_alias}.{time_spine_column} >= DATE_TRUNC('{grain}', {source_alias}.{metric_time_column})"
        )
    } else {
        // All-time cumulative: no lower bound
        String::new()
    };

    let on_expr = if lower_bound.is_empty() {
        SqlExpr::Literal(upper_bound)
    } else {
        SqlExpr::Literal(format!("{upper_bound} AND {lower_bound}"))
    };

    let join = SqlJoin {
        join_type: "INNER JOIN".into(),
        source: SqlFrom::Table {
            table: time_spine_table.to_string(),
            alias: ts_alias.clone(),
        },
        on: on_expr,
    };

    // SELECT: time_spine's ds as metric_time + source columns (except metric_time)
    let mut select_columns = vec![
        SqlExpr::Alias {
            expr: Box::new(SqlExpr::ColumnRef {
                table_alias: ts_alias.clone(),
                column_name: time_spine_column.to_string(),
            }),
            alias: "metric_time__day".to_string(),
        },
        SqlExpr::Literal(format!("{source_alias}.*")),
    ];

    Ok(SqlSelect {
        select_columns,
        from: parent_sql.from.clone(),
        joins: vec![join],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    })
}
```

- [ ] **Step 5: Run tests to verify cumulative metric plan passes**

Run: `cd metricflow-rs && cargo test -p mf-planning test_build_cumulative_metric_plan 2>&1 | tail -10`
Expected: PASS

- [ ] **Step 6: Run all tests, fix clippy/fmt, commit**

```bash
cd metricflow-rs
cargo test --all --lib
cargo fmt --all
cargo clippy --all -- -D warnings
git add crates/mf-manifest/src/graph.rs crates/mf-planning/src/builder.rs crates/mf-sql/src/convert.rs
git commit -m "feat: add cumulative metric support (JoinOverTimeRange, time spine lookup)"
```

---

## Task 10: Integration Tests for Cumulative Metrics

**Files:**
- Modify: `metricflow-rs/tests/integration.rs`

- [ ] **Step 1: Add cumulative metric with window integration test**

```rust
#[test]
fn test_end_to_end_cumulative_metric_with_window() {
    let manifest_json = include_str!("fixtures/cumulative_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["trailing_7d_bookings".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();
    eprintln!("Generated SQL (cumulative window):\n{sql}");

    // Should reference the time spine table
    assert!(
        sql.contains("mf_time_spine") || sql.contains("demo.mf_time_spine"),
        "should reference time spine: {sql}"
    );
    // Should contain an inequality join (INTERVAL or range condition)
    assert!(
        sql.contains("INTERVAL") || sql.contains("<="),
        "should have inequality join condition: {sql}"
    );
    // Should have SUM aggregation
    assert!(sql.contains("SUM"), "should have SUM: {sql}");
    assert!(
        sql.contains("metric_time__day"),
        "should have metric_time: {sql}"
    );
}
```

- [ ] **Step 2: Add grain-to-date integration test**

```rust
#[test]
fn test_end_to_end_cumulative_metric_grain_to_date() {
    let manifest_json = include_str!("fixtures/cumulative_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings_mtd".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();
    eprintln!("Generated SQL (cumulative grain_to_date):\n{sql}");

    assert!(
        sql.contains("DATE_TRUNC"),
        "should have DATE_TRUNC for grain_to_date: {sql}"
    );
    assert!(
        sql.contains("month") || sql.contains("MONTH"),
        "should reference month grain: {sql}"
    );
}
```

- [ ] **Step 3: Run all integration tests**

Run: `cd metricflow-rs && cargo test --test integration 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
cd metricflow-rs
git add tests/integration.rs
git commit -m "test: add end-to-end integration tests for cumulative metrics"
```

---

## Task 11: Update README and Roadmap

**Files:**
- Modify: `metricflow-rs/README.md`
- Modify: `docs/superpowers/plans/2026-04-01-metricflow-rs-roadmap.md`

- [ ] **Step 1: Update README status section**

Change the status section to reflect Phase 4 completion:

```markdown
## Status

**Phase 1-4 (Foundation + Simple Metrics + Joins + Derived/Cumulative)** is complete. The pipeline supports:

- Simple metrics (single measure with SUM, COUNT, AVG, etc.)
- Group-by dimensions (categorical and time dimensions on the same semantic model)
- Joins to dimensions on other semantic models (entity-based)
- Derived metrics (arbitrary SQL expressions over input metrics)
- Ratio metrics (numerator / denominator with NULLIF safety)
- Cumulative metrics (windowed, grain-to-date, and all-time)
- DuckDB SQL dialect (other dialects fall back to ANSI SQL)
- ORDER BY and LIMIT

Not yet supported (future phases):

- Conversion and offset metrics (Phase 5)
- All SQL dialects: Snowflake, BigQuery, Redshift, Postgres, Databricks, Trino (Phase 6)
- Fusion integration (Phase 7)
- SQL optimization passes (Phase 8)
```

Update the test line count accordingly.

- [ ] **Step 2: Update roadmap**

In `docs/superpowers/plans/2026-04-01-metricflow-rs-roadmap.md`, update the Phase 4 row:

```markdown
| 4 | Derived & Cumulative Metrics | 3 | Derived metric expressions, cumulative window functions, time spine joins | `2026-04-01-metricflow-rs-phase4.md` — **DONE** |
```

- [ ] **Step 3: Commit**

```bash
cd metricflow-rs
git add README.md
cd ..
git add docs/superpowers/plans/2026-04-01-metricflow-rs-roadmap.md
git commit -m "docs: update README and roadmap for Phase 4 completion"
```

---

## Notes for Implementers

1. **The `convert_combine_aggregated_outputs` function is the trickiest part.** The FULL OUTER JOIN must correctly COALESCE group-by columns and pass through metric columns from each parent. If the implementation gets complex, start with the simple case (2 parents, same group-by columns) and verify with the integration test before generalizing.

2. **Ratio metrics generate `CAST(... AS DOUBLE) / CAST(NULLIF(..., 0) AS DOUBLE)`.** The CAST ensures floating-point division (not integer). The NULLIF prevents division by zero.

3. **Cumulative metric SQL pattern:**
   ```sql
   SELECT
     time_spine.ds AS metric_time__day,
     SUM(source.__bookings) AS trailing_7d_bookings
   FROM demo.fct_bookings source
   INNER JOIN demo.mf_time_spine time_spine
     ON time_spine.ds <= source.ds
     AND time_spine.ds > source.ds - INTERVAL '7 day'
   GROUP BY time_spine.ds
   ```
   The time spine provides the output date values. Each row in the output sums all source rows within the trailing window.

4. **The `DataflowPlan` needs the `use petgraph::graph::NodeIndex;` import exposed** in `builder.rs` for the `build_input_metric_subplan` return type.

5. **Sandbox note:** Compile/test commands need `dangerouslyDisableSandbox: true` because rustc creates temp files outside the sandbox-allowed directories.

6. **All existing tests must continue to pass.** The refactoring of `build_plan()` into dispatch-by-metric-type must not break simple metric behavior.
