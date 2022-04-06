-- Join Self Over Time Range
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(revenue_src_10005.revenue) AS trailing_2_months_revenue
  , subq_8.ds__month AS ds__month
FROM (
  -- Date Spine
  SELECT
    DATE_TRUNC(ds, month) AS ds__month
  FROM ***************************.mf_time_spine subq_9
  GROUP BY
    ds__month
) subq_8
INNER JOIN (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
ON
  (
    DATE_TRUNC(revenue_src_10005.created_at, month) <= subq_8.ds__month
  ) AND (
    DATE_TRUNC(revenue_src_10005.created_at, month) > DATE_SUB(CAST(subq_8.ds__month AS DATETIME), INTERVAL 2 month)
  )
GROUP BY
  ds__month
