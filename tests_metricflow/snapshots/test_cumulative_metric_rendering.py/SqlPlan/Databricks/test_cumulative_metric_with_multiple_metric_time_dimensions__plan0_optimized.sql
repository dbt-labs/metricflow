test_name: test_cumulative_metric_with_multiple_metric_time_dimensions
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with multiple metric time dimensions.
sql_engine: Databricks
---
-- Join Self Over Time Range
-- Pass Only Elements: ['revenue', 'metric_time__day', 'metric_time__month']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_12.ds AS metric_time__day
  , DATE_TRUNC('month', subq_12.ds) AS metric_time__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_12
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_12.ds
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > DATEADD(month, -2, subq_12.ds)
  )
GROUP BY
  subq_12.ds
  , DATE_TRUNC('month', subq_12.ds)
