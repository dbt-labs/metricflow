test_name: test_cumulative_metric_with_custom_granularity
test_filename: test_custom_granularity.py
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
      subq_14.martian_day AS metric_time__martian_day
      , subq_13.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_13.ds
      ) AND (
        DATETIME_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_13.ds AS DATETIME), INTERVAL 2 month)
      )
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_14
    ON
      subq_13.ds = subq_14.ds
    GROUP BY
      metric_time__martian_day
      , metric_time__day
  ) subq_17
) subq_19
GROUP BY
  metric_time__martian_day
  , trailing_2_months_revenue
