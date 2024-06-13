-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__day', 'revenue_instance__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_9.revenue_instance__ds__day AS revenue_instance__ds__day
  , subq_9.revenue_instance__ds__month AS revenue_instance__ds__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM (
  -- Time Spine
  SELECT
    ds AS revenue_instance__ds__day
    , DATETIME_TRUNC(ds, month) AS revenue_instance__ds__month
  FROM ***************************.mf_time_spine subq_10
  GROUP BY
    revenue_instance__ds__day
    , revenue_instance__ds__month
) subq_9
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_9.revenue_instance__ds__day
  ) AND (
    DATETIME_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_9.revenue_instance__ds__day AS DATETIME), INTERVAL 2 month)
  )
GROUP BY
  revenue_instance__ds__day
  , revenue_instance__ds__month
