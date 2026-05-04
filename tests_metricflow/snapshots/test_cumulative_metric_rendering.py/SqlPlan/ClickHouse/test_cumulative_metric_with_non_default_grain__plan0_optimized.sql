test_name: test_cumulative_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative all-time metric queried with non-default grain.
sql_engine: ClickHouse
---
SELECT
  metric_time__week
  , revenue_all_time
FROM (
  SELECT
    metric_time__week
    , LAST_VALUE(revenue_all_time) OVER (
      PARTITION BY metric_time__week
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_all_time
  FROM (
    SELECT
      subq_15.ds AS metric_time__day
      , toStartOfWeek(subq_15.ds, 1) AS metric_time__week
      , SUM(revenue_src_28000.revenue) AS revenue_all_time
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (toStartOfDay(revenue_src_28000.created_at) <= subq_15.ds)
    GROUP BY
      subq_15.ds
      , toStartOfWeek(subq_15.ds, 1)
  ) subq_21
) subq_22
GROUP BY
  metric_time__week
  , revenue_all_time
