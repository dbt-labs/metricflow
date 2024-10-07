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
    subq_14.user__revenue_all_time AS user__revenue_all_time
    , subq_10.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'user']
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_10
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
  ) subq_14
  ON
    subq_10.user = subq_14.user
) subq_15
WHERE user__revenue_all_time > 1
