-- Compute Metrics via Expressions
SELECT
  metric_time__week
  , t2mr - 10 AS trailing_2_months_revenue_sub_10
FROM (
  -- Re-aggregate Metrics via Window Functions
  SELECT
    metric_time__week
    , t2mr
  FROM (
    SELECT
      metric_time__week
      , FIRST_VALUE(t2mr) OVER (
        PARTITION BY metric_time__week
        ORDER BY metric_time__day
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS t2mr
    FROM (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['txn_revenue', 'metric_time__week', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        subq_12.metric_time__day AS metric_time__day
        , subq_12.metric_time__week AS metric_time__week
        , SUM(revenue_src_28000.revenue) AS t2mr
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
          DATE_TRUNC('day', revenue_src_28000.created_at) > DATEADD(month, -2, subq_12.metric_time__day)
        )
      GROUP BY
        subq_12.metric_time__day
        , subq_12.metric_time__week
    ) subq_17
  ) subq_18
  GROUP BY
    metric_time__week
    , t2mr
) subq_19
