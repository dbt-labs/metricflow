test_name: test_all_time_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative all-time metric queried with non-default grains.

      Uses only metric_time. Excludes default grain.
sql_engine: BigQuery
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
SELECT
  metric_time__week
  , metric_time__quarter
  , revenue_all_time
FROM (
  -- Window Function for Metric Re-aggregation
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
    -- Join Self Over Time Range
    -- Pass Only Elements: ['__revenue', 'metric_time__week', 'metric_time__quarter', 'metric_time__day']
    -- Pass Only Elements: ['__revenue', 'metric_time__week', 'metric_time__quarter', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Compute Metrics via Expressions
    SELECT
      subq_15.ds AS metric_time__day
      , DATETIME_TRUNC(subq_15.ds, isoweek) AS metric_time__week
      , DATETIME_TRUNC(subq_15.ds, quarter) AS metric_time__quarter
      , SUM(revenue_src_28000.revenue) AS revenue_all_time
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_15.ds
      )
    GROUP BY
      metric_time__day
      , metric_time__week
      , metric_time__quarter
  ) subq_21
) subq_22
GROUP BY
  metric_time__week
  , metric_time__quarter
  , revenue_all_time
