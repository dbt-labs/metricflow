-- Constrain Output with WHERE
-- Pass Only Elements: ['user__home_state_latest',]
SELECT
  user__home_state_latest
FROM (
  -- Join Standard Outputs
  SELECT
    listings_latest_src_28000.country AS listing__country_latest
    , users_latest_src_28000.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    listings_latest_src_28000.user_id = users_latest_src_28000.user_id
) subq_8
WHERE listing__country_latest = 'us'
GROUP BY
  user__home_state_latest
