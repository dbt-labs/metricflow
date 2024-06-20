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
      subq_11.metric_time__day AS metric_time__day
      , subq_11.metric_time__year AS metric_time__year
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM (
      -- Time Spine
      SELECT
        ds AS metric_time__day
        , DATETIME_TRUNC(ds, year) AS metric_time__year
      FROM ***************************.mf_time_spine subq_12
      GROUP BY
        metric_time__day
        , metric_time__year
    ) subq_11
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_11.metric_time__day
      ) AND (
        DATETIME_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_11.metric_time__day AS DATETIME), INTERVAL 2 month)
      )
    GROUP BY
      metric_time__day
      , metric_time__year
  ) subq_15
) subq_17
GROUP BY
  metric_time__year
  , trailing_2_months_revenue
