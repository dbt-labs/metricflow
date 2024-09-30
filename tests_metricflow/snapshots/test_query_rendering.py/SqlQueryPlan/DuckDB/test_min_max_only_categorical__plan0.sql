-- Calculate min and max
SELECT
  MIN(subq_0.listing__country_latest) AS listing__country_latest__min
  , MAX(subq_0.listing__country_latest) AS listing__country_latest__max
FROM (
  -- Read From SemanticModelDataSet('listings_latest')
  -- Pass Only Elements: ['listing__country_latest',]
  SELECT
    listings_latest_src_28000.country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  GROUP BY
    listings_latest_src_28000.country
) subq_0
