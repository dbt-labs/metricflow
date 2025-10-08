test_name: test_derived_cumulative_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Test querying a derived metric with a cumulative input metric using non-default grains.
sql_engine: Postgres
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
        , DATE_TRUNC('week', subq_15.ds) AS metric_time__week
        , SUM(revenue_src_28000.revenue) AS revenue
      FROM ***************************.mf_time_spine subq_15
      INNER JOIN
        ***************************.fct_revenue revenue_src_28000
      ON
        (
          DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_15.ds
        ) AND (
          DATE_TRUNC('day', revenue_src_28000.created_at) > subq_15.ds - MAKE_INTERVAL(months => 2)
        )
      GROUP BY
        subq_15.ds
        , DATE_TRUNC('week', subq_15.ds)
    ) subq_18
  ) subq_21
  GROUP BY
    metric_time__week
    , t2mr
) subq_22
