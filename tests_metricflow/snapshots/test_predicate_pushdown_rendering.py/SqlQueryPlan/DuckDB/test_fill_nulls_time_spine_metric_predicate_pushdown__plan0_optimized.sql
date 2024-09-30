-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_36.metric_time__day, subq_50.metric_time__day) AS metric_time__day
    , COALESCE(subq_36.listing__country_latest, subq_50.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_36.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_50.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_34.ds AS metric_time__day
        , subq_32.listing__country_latest AS listing__country_latest
        , subq_32.bookings AS bookings
      FROM ***************************.mf_time_spine subq_34
      LEFT OUTER JOIN (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
        -- Aggregate Measures
        SELECT
          metric_time__day
          , listing__country_latest
          , SUM(bookings) AS bookings
        FROM (
          -- Join Standard Outputs
          -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
          SELECT
            subq_27.metric_time__day AS metric_time__day
            , subq_27.booking__is_instant AS booking__is_instant
            , subq_27.bookings AS bookings
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
          ) subq_27
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            subq_27.listing = listings_latest_src_28000.listing_id
        ) subq_30
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_32
      ON
        subq_34.ds = subq_32.metric_time__day
    ) subq_35
  ) subq_36
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_48.ds AS metric_time__day
        , subq_46.listing__country_latest AS listing__country_latest
        , subq_46.bookings AS bookings
      FROM ***************************.mf_time_spine subq_48
      LEFT OUTER JOIN (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
        -- Aggregate Measures
        SELECT
          metric_time__day
          , listing__country_latest
          , SUM(bookings) AS bookings
        FROM (
          -- Join Standard Outputs
          -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
          SELECT
            subq_41.metric_time__day AS metric_time__day
            , subq_41.booking__is_instant AS booking__is_instant
            , subq_41.bookings AS bookings
          FROM (
            -- Join to Time Spine Dataset
            -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
            SELECT
              subq_40.ds AS metric_time__day
              , subq_38.listing AS listing
              , subq_38.booking__is_instant AS booking__is_instant
              , subq_38.bookings AS bookings
            FROM ***************************.mf_time_spine subq_40
            INNER JOIN (
              -- Read Elements From Semantic Model 'bookings_source'
              -- Metric Time Dimension 'ds'
              SELECT
                DATE_TRUNC('day', ds) AS metric_time__day
                , listing_id AS listing
                , is_instant AS booking__is_instant
                , 1 AS bookings
              FROM ***************************.fct_bookings bookings_source_src_28000
            ) subq_38
            ON
              subq_40.ds - INTERVAL 14 day = subq_38.metric_time__day
          ) subq_41
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            subq_41.listing = listings_latest_src_28000.listing_id
        ) subq_44
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_46
      ON
        subq_48.ds = subq_46.metric_time__day
    ) subq_49
  ) subq_50
  ON
    (
      subq_36.listing__country_latest = subq_50.listing__country_latest
    ) AND (
      subq_36.metric_time__day = subq_50.metric_time__day
    )
  GROUP BY
    COALESCE(subq_36.metric_time__day, subq_50.metric_time__day)
    , COALESCE(subq_36.listing__country_latest, subq_50.listing__country_latest)
) subq_51
