-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
  , listing__country_latest
FROM (
  -- Read Elements From Data Source 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['listings', 'listing__country_latest']
  SELECT
    1 AS listings
    , country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10003
) subq_6
GROUP BY
  listing__country_latest
