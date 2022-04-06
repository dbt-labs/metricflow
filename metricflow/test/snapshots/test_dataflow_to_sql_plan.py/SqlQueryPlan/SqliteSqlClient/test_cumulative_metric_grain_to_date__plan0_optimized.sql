-- Join Self Over Time Range
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(revenue_src_10005.revenue) AS revenue_mtd
  , subq_8.ds__month AS ds__month
FROM (
  -- Date Spine
  SELECT
    '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
  FROM ***************************.mf_time_spine subq_9
  GROUP BY
    '__DATE_TRUNC_NOT_SUPPORTED__'
) subq_8
INNER JOIN (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
ON
  (
    '__DATE_TRUNC_NOT_SUPPORTED__' <= subq_8.ds__month
  ) AND (
    '__DATE_TRUNC_NOT_SUPPORTED__' >= DATE(subq_8.ds__month, 'start of month')
  )
GROUP BY
  subq_8.ds__month
