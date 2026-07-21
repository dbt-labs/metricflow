test_name: test_cumulative_metric_with_multiple_agg_time_dimensions
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with multiple agg time dimensions.
sql_engine: ClickHouse
---
SELECT
  subq_13.ds AS revenue_instance__ds__day
  , toStartOfMonth(subq_13.ds) AS revenue_instance__ds__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_13
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    toStartOfDay(revenue_src_28000.created_at) <= subq_13.ds
  ) AND (
    toStartOfDay(revenue_src_28000.created_at) > addMonths(subq_13.ds, -2)
  )
GROUP BY
  subq_13.ds
  , toStartOfMonth(subq_13.ds)
