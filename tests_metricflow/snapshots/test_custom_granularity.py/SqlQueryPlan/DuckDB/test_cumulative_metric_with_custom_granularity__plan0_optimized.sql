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
    -- Pass Only Elements: ['txn_revenue', 'metric_time__day', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    -- Aggregate Measures
    SELECT
      subq_13.martian_day AS metric_time__martian_day
      , subq_12.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM ***************************.mf_time_spine subq_12
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_12.ds
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > subq_12.ds - INTERVAL 2 month
      )
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_13
    ON
      subq_12.ds = subq_13.ds
    GROUP BY
      subq_13.martian_day
      , subq_12.ds
  ) subq_15
) subq_17
GROUP BY
  metric_time__martian_day
  , trailing_2_months_revenue
