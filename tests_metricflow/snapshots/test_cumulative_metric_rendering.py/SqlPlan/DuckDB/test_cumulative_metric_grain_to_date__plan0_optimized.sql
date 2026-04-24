test_name: test_cumulative_metric_grain_to_date
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query against a grain_to_date cumulative metric.
sql_engine: DuckDB
---
-- Read Elements From Semantic Model 'revenue'
-- Metric Time Dimension 'ds'
-- Select: ['__revenue', 'ds__day']
-- Select: ['__revenue', 'ds__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  DATE_TRUNC('day', created_at) AS ds__day
  , SUM(revenue) AS revenue_mtd
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  DATE_TRUNC('day', created_at)
