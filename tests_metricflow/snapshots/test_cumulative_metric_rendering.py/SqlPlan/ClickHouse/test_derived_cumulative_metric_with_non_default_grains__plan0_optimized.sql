test_name: test_derived_cumulative_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Test querying a derived metric with a cumulative input metric using non-default grains.
sql_engine: ClickHouse
---
SELECT
  metric_time__week
  , t2mr - 10 AS trailing_2_months_revenue_sub_10
FROM (
  SELECT
    metric_time__week
    , t2mr
  FROM (
    SELECT
      metric_time__week
      , AVG(__revenue) OVER (PARTITION BY metric_time__week) AS t2mr
    FROM (
      SELECT
        subq_16.ds AS metric_time__day
        , toStartOfWeek(subq_16.ds, 1) AS metric_time__week
        , SUM(revenue_src_28000.revenue) AS __revenue
      FROM ***************************.mf_time_spine subq_16
      INNER JOIN
        ***************************.fct_revenue revenue_src_28000
      ON
        (
          toStartOfDay(revenue_src_28000.created_at) <= subq_16.ds
        ) AND (
          toStartOfDay(revenue_src_28000.created_at) > addMonths(subq_16.ds, -2)
        )
      GROUP BY
        subq_16.ds
        , toStartOfWeek(subq_16.ds, 1)
    ) subq_20
  ) subq_23
  GROUP BY
    metric_time__week
    , t2mr
) subq_24
