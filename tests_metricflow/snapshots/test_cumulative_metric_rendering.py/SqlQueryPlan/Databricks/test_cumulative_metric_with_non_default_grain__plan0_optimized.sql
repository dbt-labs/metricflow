-- Re-aggregate Metric via Group By
SELECT
  metric_time__week
  , revenue_all_time
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__week
    , LAST_VALUE(revenue_all_time) OVER (
      PARTITION BY metric_time__week
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_all_time
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'metric_time__week', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_11.metric_time__day AS metric_time__day
      , subq_11.metric_time__week AS metric_time__week
      , SUM(revenue_src_28000.revenue) AS revenue_all_time
    FROM (
      -- Time Spine
      SELECT
        ds AS metric_time__day
        , DATE_TRUNC('week', ds) AS metric_time__week
      FROM ***************************.mf_time_spine subq_12
      GROUP BY
        ds
        , DATE_TRUNC('week', ds)
    ) subq_11
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_11.metric_time__day
      )
    GROUP BY
      subq_11.metric_time__day
      , subq_11.metric_time__week
  ) subq_16
) subq_17
GROUP BY
  metric_time__week
  , revenue_all_time
