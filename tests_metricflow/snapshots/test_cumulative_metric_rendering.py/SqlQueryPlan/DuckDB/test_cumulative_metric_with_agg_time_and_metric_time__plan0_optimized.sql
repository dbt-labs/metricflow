test_name: test_cumulative_metric_with_agg_time_and_metric_time
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with one agg time dimension and one metric time dimension.
---
-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day', 'revenue_instance__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  DATE_TRUNC('month', subq_10.ds) AS revenue_instance__ds__month
  , subq_10.ds AS metric_time__day
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_10
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_10.ds
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > subq_10.ds - INTERVAL 2 month
  )
GROUP BY
  DATE_TRUNC('month', subq_10.ds)
  , subq_10.ds
