-- Constrain Output with WHERE
-- Order By ['listing__country_latest'] Limit 100
SELECT
  listing__country_latest
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements:
  --   ['listing__country_latest']
  SELECT
    country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10004
  GROUP BY
    listing__country_latest
) subq_4
WHERE listing__country_latest = 'us'
ORDER BY listing__country_latest DESC
LIMIT 100
