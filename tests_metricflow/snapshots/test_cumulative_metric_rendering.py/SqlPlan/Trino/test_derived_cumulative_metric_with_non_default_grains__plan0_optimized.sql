test_name: test_derived_cumulative_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Test querying a derived metric with a cumulative input metric using non-default grains.
sql_engine: Trino
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
      , AVG(__revenue) OVER (PARTITION BY metric_time__week) AS t2mr
    FROM (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['__revenue', 'metric_time__week', 'metric_time__day']
      -- Pass Only Elements: ['__revenue', 'metric_time__week', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_16.ds AS metric_time__day
        , DATE_TRUNC('week', subq_16.ds) AS metric_time__week
        , SUM(revenue_src_28000.revenue) AS __revenue
      FROM ***************************.mf_time_spine subq_16
      INNER JOIN
        ***************************.fct_revenue revenue_src_28000
      ON
        (
          DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_16.ds
        ) AND (
          DATE_TRUNC('day', revenue_src_28000.created_at) > DATE_ADD('month', -2, subq_16.ds)
        )
      GROUP BY
        subq_16.ds
        , DATE_TRUNC('week', subq_16.ds)
    ) subq_20
  ) subq_23
  GROUP BY
    metric_time__week
    , t2mr
) subq_24
