test_name: test_grain_to_date_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative grain to date metric queried with non-default grain.
sql_engine: BigQuery
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
SELECT
  metric_time__month
  , revenue_mtd
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__month
    , FIRST_VALUE(revenue_mtd) OVER (
      PARTITION BY metric_time__month
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_mtd
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'metric_time__month', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_13.ds AS metric_time__day
      , TIMESTAMP_TRUNC(subq_13.ds, month) AS metric_time__month
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        TIMESTAMP_TRUNC(revenue_src_28000.created_at, day) <= subq_13.ds
      ) AND (
        TIMESTAMP_TRUNC(revenue_src_28000.created_at, day) >= TIMESTAMP_TRUNC(subq_13.ds, month)
      )
    GROUP BY
      metric_time__day
      , metric_time__month
  ) subq_17
) subq_18
GROUP BY
  metric_time__month
  , revenue_mtd
