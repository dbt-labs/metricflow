-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listings', 'user__revenue_all_time']
  SELECT
    subq_26.revenue_all_time AS user__revenue_all_time
    , subq_20.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'user']
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_20
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'revenue'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['txn_revenue', 'user']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['user', 'revenue_all_time']
    SELECT
      user_id AS user
      , SUM(revenue) AS revenue_all_time
    FROM ***************************.fct_revenue revenue_src_28000
    GROUP BY
      user
  ) subq_26
  ON
    subq_20.user = subq_26.user
) subq_28
WHERE user__revenue_all_time > 1
