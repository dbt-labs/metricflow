-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['listing__country_latest']
-- Order By ['listing__country_latest'] Limit 100
SELECT
  listing__country_latest
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10005
) subq_3
WHERE listing__country_latest = 'us'
GROUP BY
  listing__country_latest
ORDER BY listing__country_latest DESC
LIMIT 100
