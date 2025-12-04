test_name: test_cumulative_metric
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a basic cumulative metric query.
sql_engine: Postgres
---
-- Read Elements From Semantic Model 'revenue'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['__revenue', 'ds__day']
-- Pass Only Elements: ['__revenue', 'ds__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  DATE_TRUNC('day', created_at) AS ds__day
  , SUM(revenue) AS trailing_2_months_revenue
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  DATE_TRUNC('day', created_at)
