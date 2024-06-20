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
        subq_12.metric_time__day AS metric_time__day
        , subq_12.metric_time__week AS metric_time__week
        , SUM(revenue_src_28000.revenue) AS txn_revenue
      FROM (
        -- Time Spine
        SELECT
          ds AS metric_time__day
          , DATE_TRUNC('week', ds) AS metric_time__week
        FROM ***************************.mf_time_spine subq_13
        GROUP BY
          ds
          , DATE_TRUNC('week', ds)
      ) subq_12
      INNER JOIN
        ***************************.fct_revenue revenue_src_28000
      ON
        (
          DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_12.metric_time__day
        ) AND (
          DATE_TRUNC('day', revenue_src_28000.created_at) > subq_12.metric_time__day - INTERVAL 2 month
        )
      GROUP BY
        subq_12.metric_time__day
        , subq_12.metric_time__week
    ) subq_16
  ) subq_18
  GROUP BY
    metric_time__week
    , t2mr
) subq_19
