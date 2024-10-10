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
        subq_13.ds AS metric_time__day
        , DATETIME_TRUNC(subq_13.ds, isoweek) AS metric_time__week
        , SUM(revenue_src_28000.revenue) AS txn_revenue
      FROM ***************************.mf_time_spine subq_13
      INNER JOIN
        ***************************.fct_revenue revenue_src_28000
      ON
        (
          DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_13.ds
        ) AND (
          DATETIME_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_13.ds AS DATETIME), INTERVAL 2 month)
        )
      GROUP BY
        metric_time__day
        , metric_time__week
    ) subq_16
  ) subq_18
  GROUP BY
    metric_time__week
    , t2mr
) subq_19
