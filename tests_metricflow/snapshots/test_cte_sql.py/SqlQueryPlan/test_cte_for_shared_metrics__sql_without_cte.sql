-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_9.metric_time__day, subq_19.metric_time__day, subq_40.metric_time__day) AS metric_time__day
  , COALESCE(subq_9.listing__country_latest, subq_19.listing__country_latest, subq_40.listing__country_latest) AS listing__country_latest
  , MAX(subq_9.bookings) AS bookings
  , MAX(subq_19.views) AS views
  , MAX(CAST(subq_40.bookings AS DOUBLE) / CAST(NULLIF(subq_40.views, 0) AS DOUBLE)) AS bookings_per_view
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_2.metric_time__day AS metric_time__day
    , listings_latest_src_28000.country AS listing__country_latest
    , SUM(subq_2.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_2
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_2.listing = listings_latest_src_28000.listing_id
  GROUP BY
    subq_2.metric_time__day
    , listings_latest_src_28000.country
) subq_9
FULL OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['views', 'listing__country_latest', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_12.metric_time__day AS metric_time__day
    , listings_latest_src_28000.country AS listing__country_latest
    , SUM(subq_12.views) AS views
  FROM (
    -- Read Elements From Semantic Model 'views_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS views
    FROM ***************************.fct_views views_source_src_28000
  ) subq_12
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_12.listing = listings_latest_src_28000.listing_id
  GROUP BY
    subq_12.metric_time__day
    , listings_latest_src_28000.country
) subq_19
ON
  (
    subq_9.listing__country_latest = subq_19.listing__country_latest
  ) AND (
    subq_9.metric_time__day = subq_19.metric_time__day
  )
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_29.metric_time__day, subq_39.metric_time__day) AS metric_time__day
    , COALESCE(subq_29.listing__country_latest, subq_39.listing__country_latest) AS listing__country_latest
    , MAX(subq_29.bookings) AS bookings
    , MAX(subq_39.views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_22.metric_time__day AS metric_time__day
      , listings_latest_src_28000.country AS listing__country_latest
      , SUM(subq_22.bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_22
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_22.listing = listings_latest_src_28000.listing_id
    GROUP BY
      subq_22.metric_time__day
      , listings_latest_src_28000.country
  ) subq_29
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['views', 'listing__country_latest', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_32.metric_time__day AS metric_time__day
      , listings_latest_src_28000.country AS listing__country_latest
      , SUM(subq_32.views) AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_32
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_32.listing = listings_latest_src_28000.listing_id
    GROUP BY
      subq_32.metric_time__day
      , listings_latest_src_28000.country
  ) subq_39
  ON
    (
      subq_29.listing__country_latest = subq_39.listing__country_latest
    ) AND (
      subq_29.metric_time__day = subq_39.metric_time__day
    )
  GROUP BY
    COALESCE(subq_29.metric_time__day, subq_39.metric_time__day)
    , COALESCE(subq_29.listing__country_latest, subq_39.listing__country_latest)
) subq_40
ON
  (
    COALESCE(subq_9.listing__country_latest, subq_19.listing__country_latest) = subq_40.listing__country_latest
  ) AND (
    COALESCE(subq_9.metric_time__day, subq_19.metric_time__day) = subq_40.metric_time__day
  )
GROUP BY
  COALESCE(subq_9.metric_time__day, subq_19.metric_time__day, subq_40.metric_time__day)
  , COALESCE(subq_9.listing__country_latest, subq_19.listing__country_latest, subq_40.listing__country_latest)
