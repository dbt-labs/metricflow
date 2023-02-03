-- Join Self Over Time Range
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ds__month AS ds__month
  , SUM(subq_10.txn_revenue) AS trailing_2_months_revenue
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time
  FROM ***************************.mf_time_spine subq_12
  WHERE ds BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2020-01-01' AS TIMESTAMP)
  GROUP BY
    ds
) subq_11
INNER JOIN (
  -- Read Elements From Data Source 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2019-12-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATE_TRUNC('month', created_at) AS ds__month
    , created_at AS metric_time
    , revenue AS txn_revenue
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_revenue
  ) revenue_src_10006
  WHERE created_at BETWEEN CAST('2019-12-01' AS TIMESTAMP) AND CAST('2020-01-01' AS TIMESTAMP)
) subq_10
ON
  (
    subq_10.metric_time <= subq_11.metric_time
  ) AND (
    subq_10.metric_time > subq_11.metric_time - MAKE_INTERVAL(months => 2)
  )
GROUP BY
  subq_10.ds__month
