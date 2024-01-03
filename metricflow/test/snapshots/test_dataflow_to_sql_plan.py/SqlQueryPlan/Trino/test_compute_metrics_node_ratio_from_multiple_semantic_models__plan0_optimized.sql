-- Compute Metrics via Expressions
SELECT
  ds__day
  , listing__country_latest
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(views, 0) AS DOUBLE) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_30.ds__day, subq_40.ds__day) AS ds__day
    , COALESCE(subq_30.listing__country_latest, subq_40.listing__country_latest) AS listing__country_latest
    , MAX(subq_30.bookings) AS bookings
    , MAX(subq_40.views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements:
    --   ['bookings', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_23.ds__day AS ds__day
      , listings_latest_src_10005.country AS listing__country_latest
      , SUM(subq_23.bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'ds__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_23
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_10005
    ON
      subq_23.listing = listings_latest_src_10005.listing_id
    GROUP BY
      subq_23.ds__day
      , listings_latest_src_10005.country
  ) subq_30
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements:
    --   ['views', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_33.ds__day AS ds__day
      , listings_latest_src_10005.country AS listing__country_latest
      , SUM(subq_33.views) AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['views', 'ds__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_10010
    ) subq_33
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_10005
    ON
      subq_33.listing = listings_latest_src_10005.listing_id
    GROUP BY
      subq_33.ds__day
      , listings_latest_src_10005.country
  ) subq_40
  ON
    (
      subq_30.listing__country_latest = subq_40.listing__country_latest
    ) AND (
      subq_30.ds__day = subq_40.ds__day
    )
  GROUP BY
    COALESCE(subq_30.ds__day, subq_40.ds__day)
    , COALESCE(subq_30.listing__country_latest, subq_40.listing__country_latest)
) subq_41
