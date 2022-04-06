-- Join Aggregated Measures with Standard Outputs
-- Pass Only Elements:
--   ['listing__country_latest', 'ds', 'bookings', 'views']
-- Compute Metrics via Expressions
SELECT
  CAST(subq_22.bookings AS FLOAT64) / CAST(NULLIF(subq_29.views, 0) AS FLOAT64) AS bookings_per_view
  , subq_22.listing__country_latest AS listing__country_latest
  , subq_22.ds AS ds
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'listing__country_latest', 'ds']
  -- Aggregate Measures
  SELECT
    SUM(subq_17.bookings) AS bookings
    , listings_latest_src_10003.country AS listing__country_latest
    , subq_17.ds AS ds
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Pass Only Elements:
    --   ['bookings', 'ds', 'listing']
    SELECT
      1 AS bookings
      , ds
      , listing_id AS listing
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10000
  ) subq_17
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10003
  ON
    subq_17.listing = listings_latest_src_10003.listing_id
  GROUP BY
    listing__country_latest
    , ds
) subq_22
INNER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['views', 'listing__country_latest', 'ds']
  -- Aggregate Measures
  SELECT
    SUM(subq_24.views) AS views
    , listings_latest_src_10003.country AS listing__country_latest
    , subq_24.ds AS ds
  FROM (
    -- Read Elements From Data Source 'views_source'
    -- Pass Only Elements:
    --   ['views', 'ds', 'listing']
    SELECT
      1 AS views
      , ds
      , listing_id AS listing
    FROM (
      -- User Defined SQL Query
      SELECT user_id, listing_id, ds, ds_partitioned FROM ***************************.fct_views
    ) views_source_src_10008
  ) subq_24
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10003
  ON
    subq_24.listing = listings_latest_src_10003.listing_id
  GROUP BY
    listing__country_latest
    , ds
) subq_29
ON
  (
    (
      subq_22.listing__country_latest = subq_29.listing__country_latest
    ) OR (
      (
        subq_22.listing__country_latest IS NULL
      ) AND (
        subq_29.listing__country_latest IS NULL
      )
    )
  ) AND (
    (
      subq_22.ds = subq_29.ds
    ) OR (
      (subq_22.ds IS NULL) AND (subq_29.ds IS NULL)
    )
  )
