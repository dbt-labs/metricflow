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
    subq_22.user__revenue_all_time AS user__revenue_all_time
    , subq_16.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'user']
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_16
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'revenue'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['txn_revenue', 'user']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['user', 'user__revenue_all_time']
    SELECT
      user_id AS user
      , SUM(revenue) AS user__revenue_all_time
    FROM ***************************.fct_revenue revenue_src_28000
    GROUP BY
      user_id
  ) subq_22
  ON
    subq_16.user = subq_22.user
) subq_24
WHERE user__revenue_all_time > 1
