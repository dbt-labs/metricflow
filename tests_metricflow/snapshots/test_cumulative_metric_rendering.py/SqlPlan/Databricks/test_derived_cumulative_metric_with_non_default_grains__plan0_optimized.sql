test_name: test_derived_cumulative_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Test querying a derived metric with a cumulative input metric using non-default grains.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
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
    -- Window Function for Metric Re-aggregation
    SELECT
      metric_time__week
      , AVG(txn_revenue) OVER (PARTITION BY metric_time__week) AS t2mr
    FROM (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['txn_revenue', 'metric_time__week', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        nr_subq_11.ds AS metric_time__day
        , DATE_TRUNC('week', nr_subq_11.ds) AS metric_time__week
        , SUM(revenue_src_28000.revenue) AS txn_revenue
      FROM ***************************.mf_time_spine nr_subq_11
      INNER JOIN
        ***************************.fct_revenue revenue_src_28000
      ON
        (
          DATE_TRUNC('day', revenue_src_28000.created_at) <= nr_subq_11.ds
        ) AND (
          DATE_TRUNC('day', revenue_src_28000.created_at) > DATEADD(month, -2, nr_subq_11.ds)
        )
      GROUP BY
        nr_subq_11.ds
        , DATE_TRUNC('week', nr_subq_11.ds)
    ) nr_subq_14
  ) nr_subq_16
  GROUP BY
    metric_time__week
    , t2mr
) nr_subq_17
