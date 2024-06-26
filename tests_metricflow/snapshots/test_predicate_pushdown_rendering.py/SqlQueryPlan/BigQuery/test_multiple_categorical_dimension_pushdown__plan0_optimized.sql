-- Join Standard Outputs
-- Pass Only Elements: ['listings', 'user__home_state_latest', 'listing__is_lux_latest', 'listing__capacity_latest']
-- Pass Only Elements: ['listings', 'user__home_state_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  users_latest_src_28000.home_state_latest AS user__home_state_latest
  , SUM(subq_13.listings) AS listings
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['listings', 'listing__is_lux_latest', 'listing__capacity_latest', 'user']
  SELECT
    subq_11.user
    , listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , is_lux AS listing__is_lux_latest
      , capacity AS listing__capacity_latest
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_11
  WHERE listing__is_lux_latest OR listing__capacity_latest > 4
) subq_13
LEFT OUTER JOIN
  ***************************.dim_users_latest users_latest_src_28000
ON
  subq_13.user = users_latest_src_28000.user_id
GROUP BY
  user__home_state_latest
