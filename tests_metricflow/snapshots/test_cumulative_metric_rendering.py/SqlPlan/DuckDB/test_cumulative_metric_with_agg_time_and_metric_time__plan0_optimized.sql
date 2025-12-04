test_name: test_cumulative_metric_with_agg_time_and_metric_time
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with one agg time dimension and one metric time dimension.
sql_engine: DuckDB
---
-- Join Self Over Time Range
-- Pass Only Elements: ['__revenue', 'metric_time__day', 'revenue_instance__ds__month']
-- Pass Only Elements: ['__revenue', 'metric_time__day', 'revenue_instance__ds__month']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  DATE_TRUNC('month', subq_13.ds) AS revenue_instance__ds__month
  , subq_13.ds AS metric_time__day
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_13
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_13.ds
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > subq_13.ds - INTERVAL 2 month
  )
GROUP BY
  DATE_TRUNC('month', subq_13.ds)
  , subq_13.ds
