# MetricFlow Rust Rewrite

## Golden Rule

**ALWAYS check the Python MetricFlow implementation before implementing or changing any compilation/planning logic.** The Rust rewrite must match the Python behavior. The Python source is at `/Users/bper/dev/metricflow/`.

Key Python files to reference:
- `metricflow/dataflow/builder/dataflow_plan_builder.py` — main plan builder
- `metricflow/metric_evaluation/metric_query_planner.py` — metric evaluation / filter propagation
- `metricflow/metric_evaluation/dfs_me_planner.py` — depth-first metric expansion
- `metricflow/plan_conversion/node_processor.py` — predicate pushdown state
- `metricflow_semantics/specs/simple_metric_input_spec.py` — SimpleMetricRecipe with filter separation

## Build & Test

```bash
cargo build                          # Build all crates
cargo test                           # Run unit + integration tests (19 tests)
python3 tests/python_compat_test.py  # Run Python compatibility tests (55 tests, 44 non-skipped)
```

## Project Structure

- `crates/mf-core/` — Core types (manifest structs, enums)
- `crates/mf-manifest/` — Manifest parsing and SemanticGraph
- `crates/mf-planning/` — Dataflow plan building (builder, resolve, filter)
- `crates/mf-sql/` — SQL generation from dataflow plans
- `crates/mf-cli/` — CLI binary
- `tests/` — Integration tests and Python compat tests

## Current Branch

Development happens on `metricflow-rs-dev` branch.
