-- Calculate min and max
SELECT
  MIN(listing__country_latest) AS listing__country_latest__min
  , MAX(listing__country_latest) AS listing__country_latest__max
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements:
  --   ['listing__country_latest']
  SELECT
    country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10005
  GROUP BY
    country
) subq_3
