test_name: test_window_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grain.
sql_engine: ClickHouse
---
SELECT
  metric_time__year
  , trailing_2_months_revenue
FROM (
  SELECT
    metric_time__year
    , AVG(__revenue) OVER (PARTITION BY metric_time__year) AS trailing_2_months_revenue
  FROM (
    SELECT
      subq_15.ds AS metric_time__day
      , toStartOfYear(subq_15.ds) AS metric_time__year
      , SUM(revenue_src_28000.revenue) AS __revenue
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        toStartOfDay(revenue_src_28000.created_at) <= subq_15.ds
      ) AND (
        toStartOfDay(revenue_src_28000.created_at) > addMonths(subq_15.ds, -2)
      )
    GROUP BY
      subq_15.ds
      , toStartOfYear(subq_15.ds)
  ) subq_19
) subq_22
GROUP BY
  metric_time__year
  , trailing_2_months_revenue
