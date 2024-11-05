-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_33.metric_time__day, subq_46.metric_time__day) AS metric_time__day
    , COALESCE(subq_33.listing__country_latest, subq_46.listing__country_latest) AS listing__country_latest
    , MAX(subq_33.bookings) AS bookings
    , MAX(subq_46.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        listings_latest_src_28000.country AS listing__country_latest
        , subq_25.metric_time__day AS metric_time__day
        , subq_25.booking__is_instant AS booking__is_instant
        , subq_25.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , is_instant AS booking__is_instant
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_25
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_25.listing = listings_latest_src_28000.listing_id
    ) subq_29
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_33
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , SUM(bookings) AS bookings_2_weeks_ago
    FROM (
      -- Join Standard Outputs
      SELECT
        listings_latest_src_28000.country AS listing__country_latest
        , subq_38.metric_time__day AS metric_time__day
        , subq_38.booking__is_instant AS booking__is_instant
        , subq_38.bookings AS bookings
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_37.ds AS metric_time__day
          , subq_35.listing AS listing
          , subq_35.booking__is_instant AS booking__is_instant
          , subq_35.bookings AS bookings
        FROM ***************************.mf_time_spine subq_37
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , listing_id AS listing
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_35
        ON
          DATE_ADD('day', -14, subq_37.ds) = subq_35.metric_time__day
      ) subq_38
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_38.listing = listings_latest_src_28000.listing_id
    ) subq_42
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_46
  ON
    (
      subq_33.listing__country_latest = subq_46.listing__country_latest
    ) AND (
      subq_33.metric_time__day = subq_46.metric_time__day
    )
  GROUP BY
    COALESCE(subq_33.metric_time__day, subq_46.metric_time__day)
    , COALESCE(subq_33.listing__country_latest, subq_46.listing__country_latest)
) subq_47
