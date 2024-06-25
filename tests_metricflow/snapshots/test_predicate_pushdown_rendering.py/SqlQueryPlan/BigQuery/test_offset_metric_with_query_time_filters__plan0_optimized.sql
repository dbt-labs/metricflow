-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_39.metric_time__day, subq_54.metric_time__day) AS metric_time__day
    , COALESCE(subq_39.listing__country_latest, subq_54.listing__country_latest) AS listing__country_latest
    , MAX(subq_39.bookings) AS bookings
    , MAX(subq_54.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_31.metric_time__day AS metric_time__day
      , listings_latest_src_28000.country AS listing__country_latest
      , SUM(subq_31.bookings) AS bookings
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
      SELECT
        metric_time__day
        , listing
        , bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , listing_id AS listing
          , is_instant AS booking__is_instant
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_29
      WHERE booking__is_instant
    ) subq_31
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_31.listing = listings_latest_src_28000.listing_id
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_39
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
        subq_45.metric_time__day AS metric_time__day
        , subq_45.booking__is_instant AS booking__is_instant
        , listings_latest_src_28000.country AS listing__country_latest
        , subq_45.bookings AS bookings
      FROM (
        -- Join to Time Spine Dataset
        -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
        SELECT
          subq_43.ds AS metric_time__day
          , subq_41.listing AS listing
          , subq_41.booking__is_instant AS booking__is_instant
          , subq_41.bookings AS bookings
        FROM ***************************.mf_time_spine subq_43
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATETIME_TRUNC(ds, day) AS metric_time__day
            , listing_id AS listing
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_41
        ON
          DATE_SUB(CAST(subq_43.ds AS DATETIME), INTERVAL 14 day) = subq_41.metric_time__day
      ) subq_45
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_45.listing = listings_latest_src_28000.listing_id
    ) subq_50
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_54
  ON
    (
      subq_39.listing__country_latest = subq_54.listing__country_latest
    ) AND (
      subq_39.metric_time__day = subq_54.metric_time__day
    )
  GROUP BY
    metric_time__day
    , listing__country_latest
) subq_55
