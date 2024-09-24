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
    -- Pass Only Elements: ['txn_revenue', 'metric_time__day', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    -- Aggregate Measures
    SELECT
      subq_15.martian_day AS metric_time__martian_day
      , subq_14.metric_time__day AS metric_time__day
      , SUM(subq_14.txn_revenue) AS txn_revenue
    FROM (
      -- Join Self Over Time Range
      SELECT
        subq_13.ds AS metric_time__day
        , revenue_src_28000.revenue AS txn_revenue
      FROM ***************************.mf_time_spine subq_13
      INNER JOIN
        ***************************.fct_revenue revenue_src_28000
      ON
        (
          DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_13.ds
        ) AND (
          DATETIME_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_13.ds AS DATETIME), INTERVAL 2 month)
        )
    ) subq_14
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_15
    ON
      subq_14.metric_time__day = subq_15.ds
    GROUP BY
      metric_time__martian_day
      , metric_time__day
  ) subq_17
) subq_19
GROUP BY
  metric_time__martian_day
  , trailing_2_months_revenue