test_name: test_cumulative_metric_no_window
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query where there is a windowless cumulative metric to compute.
sql_engine: Databricks
---
-- Read Elements From Semantic Model 'revenue'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['__revenue', 'ds__month']
-- Pass Only Elements: ['__revenue', 'ds__month']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  DATE_TRUNC('month', created_at) AS ds__month
  , SUM(revenue) AS revenue_all_time
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  DATE_TRUNC('month', created_at)
