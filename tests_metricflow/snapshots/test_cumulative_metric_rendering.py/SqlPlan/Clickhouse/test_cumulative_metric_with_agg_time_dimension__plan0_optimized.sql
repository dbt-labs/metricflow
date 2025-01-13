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
GROUP BY
  subq_10.ds
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
