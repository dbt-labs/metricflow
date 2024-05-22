-- Constrain Output with WHERE
-- Pass Only Elements: ['user__home_state_latest',]
SELECT
  user__home_state_latest
FROM (
  -- Join Standard Outputs
  SELECT
    subq_9.listing__country_latest AS listing__country_latest
    , users_latest_src_28000.home_state_latest AS user__home_state_latest
  FROM (
    -- Constrain Output with WHERE
    SELECT
      subq_8.user
      , listing__country_latest
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      SELECT
        country AS listing__country_latest
        , user_id AS user
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_8
    WHERE listing__country_latest = 'us'
  ) subq_9
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    subq_9.user = users_latest_src_28000.user_id
) subq_12
WHERE listing__country_latest = 'us'
GROUP BY
  user__home_state_latest
