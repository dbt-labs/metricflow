test_name: test_grain_to_date_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative grain to date metric queried with non-default grain.
sql_engine: ClickHouse
---
SELECT
  metric_time__month
  , revenue_mtd
FROM (
  SELECT
    metric_time__month
    , FIRST_VALUE(revenue_mtd) OVER (
      PARTITION BY metric_time__month
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_mtd
  FROM (
    SELECT
      subq_15.ds AS metric_time__day
      , toStartOfMonth(subq_15.ds) AS metric_time__month
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        toStartOfDay(revenue_src_28000.created_at) <= subq_15.ds
      ) AND (
        toStartOfDay(revenue_src_28000.created_at) >= toStartOfMonth(subq_15.ds)
      )
    GROUP BY
      subq_15.ds
      , toStartOfMonth(subq_15.ds)
  ) subq_21
) subq_22
GROUP BY
  metric_time__month
  , revenue_mtd
