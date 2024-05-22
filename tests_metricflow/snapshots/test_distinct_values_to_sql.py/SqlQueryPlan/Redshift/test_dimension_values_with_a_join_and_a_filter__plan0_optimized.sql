-- Constrain Output with WHERE
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest']
SELECT
  listing__is_lux_latest
  , user__home_state_latest
FROM (
  -- Join Standard Outputs
  SELECT
    listings_latest_src_28000.is_lux AS listing__is_lux_latest
    , subq_11.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['home_state_latest', 'user']
    SELECT
      subq_9.user
      , home_state_latest
    FROM (
      -- Read Elements From Semantic Model 'users_latest'
      SELECT
        home_state_latest
        , home_state_latest AS user__home_state_latest
        , user_id AS user
      FROM ***************************.dim_users_latest users_latest_src_28000
    ) subq_9
    WHERE user__home_state_latest = 'us'
  ) subq_11
  ON
    listings_latest_src_28000.user_id = subq_11.user
) subq_12
WHERE user__home_state_latest = 'us'
GROUP BY
  listing__is_lux_latest
  , user__home_state_latest
