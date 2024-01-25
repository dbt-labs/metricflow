-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__day']
-- Constrain Time Range to [2020-03-05T00:00:00, 2021-01-04T00:00:00]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_12.revenue_instance__ds__day AS revenue_instance__ds__day
  , SUM(subq_11.txn_revenue) AS trailing_2_months_revenue
FROM (
  -- Time Spine
  SELECT
    ds AS revenue_instance__ds__day
  FROM ***************************.mf_time_spine subq_13
  WHERE ds BETWEEN '2020-03-05' AND '2021-01-04'
) subq_12
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-05T00:00:00, 2021-01-04T00:00:00]
  SELECT
    DATE_TRUNC('day', created_at) AS revenue_instance__ds__day
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_10007
  WHERE DATE_TRUNC('day', created_at) BETWEEN '2020-01-05' AND '2021-01-04'
) subq_11
ON
  (
    subq_11.revenue_instance__ds__day <= subq_12.revenue_instance__ds__day
  ) AND (
    subq_11.revenue_instance__ds__day > subq_12.revenue_instance__ds__day - INTERVAL 2 month
  )
WHERE subq_12.revenue_instance__ds__day BETWEEN '2020-03-05' AND '2021-01-04'
GROUP BY
  subq_12.revenue_instance__ds__day
