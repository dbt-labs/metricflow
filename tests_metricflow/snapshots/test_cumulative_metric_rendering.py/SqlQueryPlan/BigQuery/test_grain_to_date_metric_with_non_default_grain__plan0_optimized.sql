-- Re-aggregate Metrics via Window Functions
SELECT
  metric_time__month
  , revenue_mtd
FROM (
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
      subq_11.metric_time__day AS metric_time__day
      , subq_11.metric_time__month AS metric_time__month
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM (
      -- Time Spine
      SELECT
        ds AS metric_time__day
        , DATETIME_TRUNC(ds, month) AS metric_time__month
      FROM ***************************.mf_time_spine subq_12
      GROUP BY
        metric_time__day
        , metric_time__month
    ) subq_11
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_11.metric_time__day
      ) AND (
        DATETIME_TRUNC(revenue_src_28000.created_at, day) >= DATETIME_TRUNC(subq_11.metric_time__day, month)
      )
    GROUP BY
      metric_time__day
      , metric_time__month
  ) subq_16
) subq_17
GROUP BY
  metric_time__month
  , revenue_mtd
