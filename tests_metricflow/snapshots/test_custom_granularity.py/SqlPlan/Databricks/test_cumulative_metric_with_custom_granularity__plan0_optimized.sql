test_name: test_cumulative_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Re-aggregate Metric via Group By
SELECT
  metric_time__martian_day
  , trailing_2_months_revenue
FROM (
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__martian_day
    , AVG(txn_revenue) OVER (PARTITION BY metric_time__martian_day) AS trailing_2_months_revenue
  FROM (
    -- Join Self Over Time Range
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['txn_revenue', 'metric_time__martian_day', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      nr_subq_12.martian_day AS metric_time__martian_day
      , nr_subq_11.ds AS metric_time__day
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
    LEFT OUTER JOIN
      ***************************.mf_time_spine nr_subq_12
    ON
      nr_subq_11.ds = nr_subq_12.ds
    GROUP BY
      nr_subq_12.martian_day
      , nr_subq_11.ds
  ) nr_subq_15
) nr_subq_17
GROUP BY
  metric_time__martian_day
  , trailing_2_months_revenue
