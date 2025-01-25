test_name: test_cumulative_metric_with_multiple_agg_time_dimensions
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with multiple agg time dimensions.
sql_engine: Redshift
---
-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__day', 'revenue_instance__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  nr_subq_8.ds AS revenue_instance__ds__day
  , DATE_TRUNC('month', nr_subq_8.ds) AS revenue_instance__ds__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine nr_subq_8
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= nr_subq_8.ds
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > DATEADD(month, -2, nr_subq_8.ds)
  )
GROUP BY
  nr_subq_8.ds
  , DATE_TRUNC('month', nr_subq_8.ds)
