-- Calculate min and max
SELECT
  MIN(listing__country_latest) AS min_listing__country_latest
  , MAX(listing__country_latest) AS max_listing__country_latest
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements:
  --   ['listing__country_latest']
  SELECT
    country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10004
  GROUP BY
    country
) subq_3
