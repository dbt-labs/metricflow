-- Join Aggregated Measures with Standard Outputs
-- Pass Only Elements:
--   ['bookings', 'views', 'listing__country_latest', 'ds']
-- Compute Metrics via Expressions
SELECT
  subq_28.ds AS ds
  , subq_28.listing__country_latest AS listing__country_latest
  , CAST(subq_28.bookings AS FLOAT64) / CAST(NULLIF(subq_37.views, 0) AS FLOAT64) AS bookings_per_view
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'listing__country_latest', 'ds']
  -- Aggregate Measures
  SELECT
    subq_22.ds AS ds
    , listings_latest_src_10004.country AS listing__country_latest
    , SUM(subq_22.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'ds', 'listing']
    SELECT
      ds
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_22
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_22.listing = listings_latest_src_10004.listing_id
  GROUP BY
    ds
    , listing__country_latest
) subq_28
INNER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['views', 'listing__country_latest', 'ds']
  -- Aggregate Measures
  SELECT
    subq_31.ds AS ds
    , listings_latest_src_10004.country AS listing__country_latest
    , SUM(subq_31.views) AS views
  FROM (
    -- Read Elements From Semantic Model 'views_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['views', 'ds', 'listing']
    SELECT
      ds
      , listing_id AS listing
      , 1 AS views
    FROM ***************************.fct_views views_source_src_10009
  ) subq_31
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_31.listing = listings_latest_src_10004.listing_id
  GROUP BY
    ds
    , listing__country_latest
) subq_37
ON
  (
    (
      subq_28.ds = subq_37.ds
    ) OR (
      (subq_28.ds IS NULL) AND (subq_37.ds IS NULL)
    )
  ) AND (
    (
      subq_28.listing__country_latest = subq_37.listing__country_latest
    ) OR (
      (
        subq_28.listing__country_latest IS NULL
      ) AND (
        subq_37.listing__country_latest IS NULL
      )
    )
  )
