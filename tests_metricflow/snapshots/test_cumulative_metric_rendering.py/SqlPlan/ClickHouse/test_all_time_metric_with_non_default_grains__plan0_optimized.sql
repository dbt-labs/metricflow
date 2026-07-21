test_name: test_all_time_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative all-time metric queried with non-default grains.

      Uses only metric_time. Excludes default grain.
sql_engine: ClickHouse
---
SELECT
  metric_time__week
  , metric_time__quarter
  , revenue_all_time
FROM (
  SELECT
    metric_time__week
    , metric_time__quarter
    , LAST_VALUE(revenue_all_time) OVER (
      PARTITION BY
        metric_time__week
        , metric_time__quarter
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_all_time
  FROM (
    SELECT
      subq_15.ds AS metric_time__day
      , toStartOfWeek(subq_15.ds, 1) AS metric_time__week
      , toStartOfQuarter(subq_15.ds) AS metric_time__quarter
      , SUM(revenue_src_28000.revenue) AS revenue_all_time
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (toStartOfDay(revenue_src_28000.created_at) <= subq_15.ds)
    GROUP BY
      subq_15.ds
      , toStartOfWeek(subq_15.ds, 1)
      , toStartOfQuarter(subq_15.ds)
  ) subq_21
) subq_22
GROUP BY
  metric_time__week
  , metric_time__quarter
  , revenue_all_time
