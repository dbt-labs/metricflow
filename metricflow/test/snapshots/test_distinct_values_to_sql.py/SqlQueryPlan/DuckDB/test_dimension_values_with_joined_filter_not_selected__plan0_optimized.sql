-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['listing__is_lux_latest']
SELECT
  listing__is_lux_latest
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    is_lux AS listing__is_lux_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10004
) subq_2
WHERE user__home_state_latest = 'us'
GROUP BY
  listing__is_lux_latest
