test_name: test_cumulative_metric_with_multiple_metric_time_dimensions
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with multiple metric time dimensions.
sql_engine: DuckDB
---
-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day', 'metric_time__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  nr_subq_8.ds AS metric_time__day
  , DATE_TRUNC('month', nr_subq_8.ds) AS metric_time__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine nr_subq_8
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= nr_subq_8.ds
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > nr_subq_8.ds - INTERVAL 2 month
  )
GROUP BY
  nr_subq_8.ds
  , DATE_TRUNC('month', nr_subq_8.ds)
