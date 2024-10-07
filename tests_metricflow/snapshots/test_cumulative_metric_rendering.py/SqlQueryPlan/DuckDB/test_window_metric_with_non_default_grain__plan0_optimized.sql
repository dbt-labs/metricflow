-- Re-aggregate Metric via Group By
SELECT
  metric_time__year
  , trailing_2_months_revenue
FROM (
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__year
    , AVG(txn_revenue) OVER (PARTITION BY metric_time__year) AS trailing_2_months_revenue
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'metric_time__year', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_10.metric_time__day AS metric_time__day
      , subq_10.metric_time__year AS metric_time__year
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM (
      -- Time Spine
      SELECT
        ds AS metric_time__day
        , DATE_TRUNC('year', ds) AS metric_time__year
      FROM ***************************.mf_time_spine subq_11
      GROUP BY
        ds
        , DATE_TRUNC('year', ds)
    ) subq_10
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_10.metric_time__day
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > subq_10.metric_time__day - INTERVAL 2 month
      )
    GROUP BY
      subq_10.metric_time__day
      , subq_10.metric_time__year
  ) subq_13
) subq_15
GROUP BY
  metric_time__year
  , trailing_2_months_revenue
