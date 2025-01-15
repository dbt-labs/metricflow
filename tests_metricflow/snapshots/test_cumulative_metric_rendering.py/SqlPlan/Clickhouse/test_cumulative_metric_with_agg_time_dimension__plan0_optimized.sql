test_name: test_cumulative_metric_with_agg_time_dimension
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with agg time dimension.
sql_engine: Clickhouse
---
-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ds AS revenue_instance__ds__day
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_10
CROSS JOIN
  ***************************.fct_revenue revenue_src_28000
WHERE ((
  date_trunc('day', revenue_src_28000.created_at) <= subq_10.ds
) AND (
  date_trunc('day', revenue_src_28000.created_at) > DATEADD(month, -2, subq_10.ds)
))
GROUP BY
  revenue_instance__ds__day
