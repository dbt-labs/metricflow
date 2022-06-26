-- Join Aggregated Measures with Standard Outputs
-- Pass Only Elements:
--   ['listing__country_latest', 'ds', 'bookings', 'views']
-- Compute Metrics via Expressions
SELECT
  CAST(subq_28.bookings AS DOUBLE) / CAST(NULLIF(subq_37.views, 0) AS DOUBLE) AS bookings_per_view
  , subq_28.listing__country_latest AS listing__country_latest
  , subq_28.ds AS ds
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'listing__country_latest', 'ds']
  -- Aggregate Measures
  SELECT
    SUM(subq_22.bookings) AS bookings
    , listings_latest_src_10003.country AS listing__country_latest
    , subq_22.ds AS ds
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
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
  ) subq_22
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10003
  ON
    subq_22.listing = listings_latest_src_10003.listing_id
  GROUP BY
    listings_latest_src_10003.country
    , subq_22.ds
) subq_28
INNER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['views', 'listing__country_latest', 'ds']
  -- Aggregate Measures
  SELECT
    SUM(subq_31.views) AS views
    , listings_latest_src_10003.country AS listing__country_latest
    , subq_31.ds AS ds
  FROM (
    -- Read Elements From Data Source 'views_source'
    -- Metric Time Dimension 'ds'
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
  ) subq_31
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10003
  ON
    subq_31.listing = listings_latest_src_10003.listing_id
  GROUP BY
    listings_latest_src_10003.country
    , subq_31.ds
) subq_37
ON
  (
    (
      subq_28.listing__country_latest = subq_37.listing__country_latest
    ) OR (
      (
        subq_28.listing__country_latest IS NULL
      ) AND (
        subq_37.listing__country_latest IS NULL
      )
    )
  ) AND (
    (
      subq_28.ds = subq_37.ds
    ) OR (
      (subq_28.ds IS NULL) AND (subq_37.ds IS NULL)
    )
  )
