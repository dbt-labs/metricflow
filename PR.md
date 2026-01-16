# Summary
- Support `metric_time` in metric filter `group_by` so the filter metric aligns to the parent query's time grain.
- Parse metric filter group_bys into `GroupByItemReference` (entity links + optional time grain) and use those in MetricFlow group-by metric resolution.
- Add a query rendering test that exercises `metric_time__day` in a filter with a `metric_time__month` query grain.
- Add `repeat_customer_orders` to the jaffle template as an example metric-time filter.

# Behavior
- When `metric_time` is included in a metric filter group_by, joins align on `metric_time` at the query grain.
- When the query has no time dimension, `metric_time` in the filter group_by is ignored (backward compatible).
- Metric filter group_bys only allow `metric_time` as a time dimension; other time-grain group_by items are rejected with a clear error.

# Testing
- `make lint` (dbt-semantic-interfaces)
- `make test` (dbt-semantic-interfaces)
- `make lint` (metricflow)
- `make test ADDITIONAL_PYTEST_OPTIONS=--overwrite-snapshots` (metricflow)
- `make test` (metricflow)
- `DBT_PROFILES_DIR=/tmp/jaffle_profiles DBT_PROJECT_DIR=/Users/WTremml/Documents/Github/MetricFlowWrk/jaffle-sl-template hatch run dev-env:dbt parse` (from `metricflow/dbt-metricflow`)
- `DBT_PROFILES_DIR=/tmp/jaffle_profiles DBT_PROJECT_DIR=/Users/WTremml/Documents/Github/MetricFlowWrk/jaffle-sl-template hatch run dev-env:mf query --metrics repeat_customer_orders --group-by metric_time__month --explain` (from `metricflow/dbt-metricflow`)

# Notes
- The `metricflow-semantics`, `metricflow`, and `dbt-metricflow` dev envs use the local `dbt-semantic-interfaces` checkout (installed editable) to pick up group_by parsing changes; this can produce dependency warnings because `dbt-core` and `metricflow` pin older DSI versions.
- `dbt parse` emits a deprecation warning for cumulative metric params and a warning about `average_revenue` expr; these are pre-existing in the jaffle project.
- A temporary `profiles.yml` was created under `/tmp/jaffle_profiles` for the local DuckDB run.
