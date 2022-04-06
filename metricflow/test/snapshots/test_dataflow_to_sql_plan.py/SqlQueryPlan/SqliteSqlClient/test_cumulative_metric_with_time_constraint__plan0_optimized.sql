-- Join Self Over Time Range
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(subq_10.txn_revenue) AS trailing_2_months_revenue
  , subq_11.ds__month AS ds__month
FROM (
  -- Date Spine
  SELECT
    '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
  FROM ***************************.mf_time_spine subq_12
  WHERE (
    ds >= CAST('2020-01-01' AS TEXT)
  ) AND (
    ds <= CAST('2020-01-01' AS TEXT)
  )
  GROUP BY
    '__DATE_TRUNC_NOT_SUPPORTED__'
) subq_11
INNER JOIN (
  -- Read Elements From Data Source 'revenue'
  -- Constrain Time Range to [2019-12-01T00:00:00, 2020-01-01T00:00:00]
  -- Pass Only Elements:
  --   ['txn_revenue', 'ds__month']
  SELECT
    revenue AS txn_revenue
    , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_revenue
  ) revenue_src_10005
  WHERE (
    created_at >= CAST('2019-12-01' AS TEXT)
  ) AND (
    created_at <= CAST('2020-01-01' AS TEXT)
  )
) subq_10
ON
  (
    subq_10.ds__month <= subq_11.ds__month
  ) AND (
    subq_10.ds__month > DATE(subq_11.ds__month, '-2 month')
  )
WHERE (
  subq_11.ds__month >= CAST('2020-01-01' AS TEXT)
) AND (
  subq_11.ds__month <= CAST('2020-01-01' AS TEXT)
)
GROUP BY
  subq_11.ds__month
