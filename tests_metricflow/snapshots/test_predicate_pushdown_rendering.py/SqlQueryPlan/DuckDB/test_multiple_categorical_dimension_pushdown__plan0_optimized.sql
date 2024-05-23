-- Constrain Output with WHERE
-- Pass Only Elements: ['listings', 'user__home_state_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  user__home_state_latest
  , SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listings', 'user__home_state_latest', 'listing__is_lux_latest', 'listing__capacity_latest']
  SELECT
    subq_16.listing__is_lux_latest AS listing__is_lux_latest
    , subq_16.listing__capacity_latest AS listing__capacity_latest
    , users_latest_src_28000.home_state_latest AS user__home_state_latest
    , subq_16.listings AS listings
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['listings', 'listing__is_lux_latest', 'listing__capacity_latest', 'user']
    SELECT
      subq_14.user
      , listing__is_lux_latest
      , listing__capacity_latest
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
    ) subq_14
    WHERE listing__is_lux_latest OR listing__capacity_latest > 4
  ) subq_16
  LEFT OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    subq_16.user = users_latest_src_28000.user_id
) subq_20
WHERE listing__is_lux_latest OR listing__capacity_latest > 4
GROUP BY
  user__home_state_latest
