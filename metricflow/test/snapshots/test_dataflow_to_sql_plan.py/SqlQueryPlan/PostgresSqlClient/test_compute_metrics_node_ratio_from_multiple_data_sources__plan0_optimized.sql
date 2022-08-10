-- Join Aggregated Measures with Standard Outputs
-- Pass Only Elements:
--   ['listing__country_latest', 'ds', 'bookings', 'views']
-- Compute Metrics via Expressions
SELECT
  subq_34.ds AS ds
  , subq_34.listing__country_latest AS listing__country_latest
  , CAST(subq_34.bookings AS DOUBLE PRECISION) / CAST(NULLIF(subq_45.views, 0) AS DOUBLE PRECISION) AS bookings_per_view
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'listing__country_latest', 'ds']
  -- Aggregate Measures
  SELECT
    subq_27.ds AS ds
    , listings_latest_src_10004.country AS listing__country_latest
    , SUM(subq_27.bookings) AS bookings
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Pass Only Additive Measures
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'ds', 'listing']
    SELECT
      ds
      , listing_id AS listing
      , 1 AS bookings
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10001
  ) subq_27
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_27.listing = listings_latest_src_10004.listing_id
  GROUP BY
    subq_27.ds
    , listings_latest_src_10004.country
) subq_34
INNER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['views', 'listing__country_latest', 'ds']
  -- Aggregate Measures
  SELECT
    subq_38.ds AS ds
    , listings_latest_src_10004.country AS listing__country_latest
    , SUM(subq_38.views) AS views
  FROM (
    -- Read Elements From Data Source 'views_source'
    -- Pass Only Additive Measures
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['views', 'ds', 'listing']
    SELECT
      ds
      , listing_id AS listing
      , 1 AS views
    FROM (
      -- User Defined SQL Query
      SELECT user_id, listing_id, ds, ds_partitioned FROM ***************************.fct_views
    ) views_source_src_10009
  ) subq_38
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_38.listing = listings_latest_src_10004.listing_id
  GROUP BY
    subq_38.ds
    , listings_latest_src_10004.country
) subq_45
ON
  (
    (
      subq_34.listing__country_latest = subq_45.listing__country_latest
    ) OR (
      (
        subq_34.listing__country_latest IS NULL
      ) AND (
        subq_45.listing__country_latest IS NULL
      )
    )
  ) AND (
    (
      subq_34.ds = subq_45.ds
    ) OR (
      (subq_34.ds IS NULL) AND (subq_45.ds IS NULL)
    )
  )
