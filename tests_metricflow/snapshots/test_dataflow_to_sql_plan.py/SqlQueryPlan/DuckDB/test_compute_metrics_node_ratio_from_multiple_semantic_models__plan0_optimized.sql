-- Compute Metrics via Expressions
SELECT
  ds__day
  , listing__country_latest
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(views, 0) AS DOUBLE) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.ds__day, subq_28.ds__day) AS ds__day
    , COALESCE(subq_21.listing__country_latest, subq_28.listing__country_latest) AS listing__country_latest
    , MAX(subq_21.bookings) AS bookings
    , MAX(subq_28.views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_16.ds__day AS ds__day
      , subq_19.listing__country_latest AS listing__country_latest
      , SUM(subq_16.bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'ds__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_16
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_16.listing = listings_latest_src_28000.listing_id
    GROUP BY
      subq_16.ds__day
      , subq_19.listing__country_latest
  ) subq_21
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['views', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_23.ds__day AS ds__day
      , subq_26.listing__country_latest AS listing__country_latest
      , SUM(subq_23.views) AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['views', 'ds__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_23
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_23.listing = listings_latest_src_28000.listing_id
    GROUP BY
      subq_23.ds__day
      , subq_26.listing__country_latest
  ) subq_28
  ON
    (
      subq_21.listing__country_latest = subq_28.listing__country_latest
    ) AND (
      subq_21.ds__day = subq_28.ds__day
    )
  GROUP BY
    COALESCE(subq_21.ds__day, subq_28.ds__day)
    , COALESCE(subq_21.listing__country_latest, subq_28.listing__country_latest)
) subq_29
