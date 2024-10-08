-- Re-aggregate Metric via Group By
SELECT
  metric_time__week
  , metric_time__quarter
  , revenue_all_time
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__week
    , metric_time__quarter
    , LAST_VALUE(revenue_all_time) OVER (
      PARTITION BY
        metric_time__week
        , metric_time__quarter
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_all_time
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'metric_time__week', 'metric_time__quarter', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_12.ds AS metric_time__day
      , DATETIME_TRUNC(subq_12.ds, isoweek) AS metric_time__week
      , DATETIME_TRUNC(subq_12.ds, quarter) AS metric_time__quarter
      , SUM(revenue_src_28000.revenue) AS revenue_all_time
    FROM ***************************.mf_time_spine subq_12
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_12.ds
      )
    GROUP BY
      metric_time__day
      , metric_time__week
      , metric_time__quarter
  ) subq_16
) subq_17
GROUP BY
  metric_time__week
  , metric_time__quarter
  , revenue_all_time
