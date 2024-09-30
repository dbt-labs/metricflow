-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__day, subq_38.metric_time__day) AS metric_time__day
    , COALESCE(subq_27.listing__country_latest, subq_38.listing__country_latest) AS listing__country_latest
    , MAX(subq_27.bookings) AS bookings
    , MAX(subq_38.bookings_2_weeks_ago) AS bookings_2_weeks_ago
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
      -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
      SELECT
        subq_21.metric_time__day AS metric_time__day
        , subq_21.booking__is_instant AS booking__is_instant
        , subq_21.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , is_instant AS booking__is_instant
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_21
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_21.listing = listings_latest_src_28000.listing_id
    ) subq_24
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_27
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
      -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
      SELECT
        subq_32.metric_time__day AS metric_time__day
        , subq_32.booking__is_instant AS booking__is_instant
        , subq_32.bookings AS bookings
      FROM (
        -- Join to Time Spine Dataset
        -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
        SELECT
          subq_31.ds AS metric_time__day
          , subq_29.listing AS listing
          , subq_29.booking__is_instant AS booking__is_instant
          , subq_29.bookings AS bookings
        FROM ***************************.mf_time_spine subq_31
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , listing_id AS listing
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_29
        ON
          subq_31.ds - INTERVAL 14 day = subq_29.metric_time__day
      ) subq_32
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_32.listing = listings_latest_src_28000.listing_id
    ) subq_35
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_38
  ON
    (
      subq_27.listing__country_latest = subq_38.listing__country_latest
    ) AND (
      subq_27.metric_time__day = subq_38.metric_time__day
    )
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_38.metric_time__day)
    , COALESCE(subq_27.listing__country_latest, subq_38.listing__country_latest)
) subq_39
