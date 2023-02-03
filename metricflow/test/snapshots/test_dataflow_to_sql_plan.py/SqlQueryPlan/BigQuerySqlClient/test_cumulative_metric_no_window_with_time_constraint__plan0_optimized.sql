-- Join Self Over Time Range
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ds__month AS ds__month
  , SUM(subq_10.txn_revenue) AS revenue_all_time
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time
  FROM ***************************.mf_time_spine subq_12
  WHERE ds BETWEEN CAST('2020-01-01' AS DATETIME) AND CAST('2020-01-01' AS DATETIME)
  GROUP BY
    metric_time
) subq_11
INNER JOIN (
  -- Read Elements From Data Source 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATE_TRUNC(created_at, month) AS ds__month
    , created_at AS metric_time
    , revenue AS txn_revenue
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_revenue
  ) revenue_src_10006
  WHERE created_at BETWEEN CAST('2000-01-01' AS DATETIME) AND CAST('2020-01-01' AS DATETIME)
) subq_10
ON
  (subq_10.metric_time <= subq_11.metric_time)
GROUP BY
  ds__month
