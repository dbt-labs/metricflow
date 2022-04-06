-- Join Self Over Time Range
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(revenue_src_10005.revenue) AS trailing_2_months_revenue
  , subq_8.ds__month AS ds__month
FROM (
  -- Date Spine
  SELECT
    DATE_TRUNC('month', ds) AS ds__month
  FROM ***************************.mf_time_spine subq_9
  GROUP BY
    DATE_TRUNC('month', ds)
) subq_8
INNER JOIN (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
ON
  (
    DATE_TRUNC('month', revenue_src_10005.created_at) <= subq_8.ds__month
  ) AND (
    DATE_TRUNC('month', revenue_src_10005.created_at) > DATEADD(month, -2, subq_8.ds__month)
  )
GROUP BY
  subq_8.ds__month
