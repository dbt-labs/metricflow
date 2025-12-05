test_name: test_cumulative_metric_with_agg_time_dimension
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with agg time dimension.
sql_engine: Postgres
---
-- Join Self Over Time Range
-- Pass Only Elements: ['__revenue', 'revenue_instance__ds__day']
-- Pass Only Elements: ['__revenue', 'revenue_instance__ds__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_13.ds AS revenue_instance__ds__day
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_13
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_13.ds
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > subq_13.ds - MAKE_INTERVAL(months => 2)
  )
GROUP BY
  subq_13.ds
