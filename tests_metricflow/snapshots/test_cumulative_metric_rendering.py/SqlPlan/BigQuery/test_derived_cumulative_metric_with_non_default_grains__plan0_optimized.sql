test_name: test_derived_cumulative_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Test querying a derived metric with a cumulative input metric using non-default grains.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__week
  , t2mr - 10 AS trailing_2_months_revenue_sub_10
FROM (
  -- Re-aggregate Metric via Group By
  SELECT
    metric_time__week
    , t2mr
  FROM (
    -- Compute Metrics via Expressions
    -- Compute Metrics via Expressions
    -- Window Function for Metric Re-aggregation
    SELECT
      metric_time__week
      , AVG(revenue) OVER (PARTITION BY metric_time__week) AS t2mr
    FROM (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['revenue', 'metric_time__week', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_15.ds AS metric_time__day
        , DATETIME_TRUNC(subq_15.ds, isoweek) AS metric_time__week
        , SUM(revenue_src_28000.revenue) AS revenue
      FROM ***************************.mf_time_spine subq_15
      INNER JOIN
        ***************************.fct_revenue revenue_src_28000
      ON
        (
          DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_15.ds
        ) AND (
          DATETIME_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_15.ds AS DATETIME), INTERVAL 2 month)
        )
      GROUP BY
        metric_time__day
        , metric_time__week
    ) subq_18
  ) subq_21
  GROUP BY
    metric_time__week
    , t2mr
) subq_22
