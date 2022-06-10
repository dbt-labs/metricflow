-- Join Self Over Time Range
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(subq_10.txn_revenue) AS revenue_all_time
  , subq_11.ds__month AS ds__month
FROM (
  -- Date Spine
  SELECT
    DATE_TRUNC('month', ds) AS ds__month
  FROM ***************************.mf_time_spine subq_12
  WHERE (
    ds >= CAST('2020-01-01' AS TIMESTAMP)
  ) AND (
    ds <= CAST('2020-01-01' AS TIMESTAMP)
  )
  GROUP BY
    DATE_TRUNC('month', ds)
) subq_11
INNER JOIN (
  -- Read Elements From Data Source 'revenue'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
  -- Pass Only Elements:
  --   ['txn_revenue', 'ds__month']
  SELECT
    revenue AS txn_revenue
    , DATE_TRUNC('month', created_at) AS ds__month
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_revenue
  ) revenue_src_10005
  WHERE (
    created_at >= CAST('2000-01-01' AS TIMESTAMP)
  ) AND (
    created_at <= CAST('2020-01-01' AS TIMESTAMP)
  )
) subq_10
ON
  subq_10.ds__month <= subq_11.ds__month
WHERE (
  subq_11.ds__month >= CAST('2020-01-01' AS TIMESTAMP)
) AND (
  subq_11.ds__month <= CAST('2020-01-01' AS TIMESTAMP)
)
GROUP BY
  subq_11.ds__month
