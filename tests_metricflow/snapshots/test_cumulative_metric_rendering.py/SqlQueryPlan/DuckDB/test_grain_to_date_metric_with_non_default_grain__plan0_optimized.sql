-- Re-aggregate Metric via Group By
SELECT
  metric_time__month
  , revenue_mtd
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__month
    , FIRST_VALUE(revenue_mtd) OVER (
      PARTITION BY metric_time__month
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_mtd
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'metric_time__month', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_10.metric_time__day AS metric_time__day
      , subq_10.metric_time__month AS metric_time__month
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM (
      -- Time Spine
      SELECT
        ds AS metric_time__day
        , DATE_TRUNC('month', ds) AS metric_time__month
      FROM ***************************.mf_time_spine subq_11
      GROUP BY
        ds
        , DATE_TRUNC('month', ds)
    ) subq_10
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_10.metric_time__day
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) >= DATE_TRUNC('month', subq_10.metric_time__day)
      )
    GROUP BY
      subq_10.metric_time__day
      , subq_10.metric_time__month
  ) subq_14
) subq_15
GROUP BY
  metric_time__month
  , revenue_mtd
