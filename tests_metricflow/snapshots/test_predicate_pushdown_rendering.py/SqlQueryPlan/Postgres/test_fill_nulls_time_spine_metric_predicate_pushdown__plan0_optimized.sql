-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_48.metric_time__day, subq_66.metric_time__day) AS metric_time__day
    , COALESCE(subq_48.listing__country_latest, subq_66.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_48.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_66.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_46.ds AS metric_time__day
        , subq_44.listing__country_latest AS listing__country_latest
        , subq_44.bookings AS bookings
      FROM ***************************.mf_time_spine subq_46
      LEFT OUTER JOIN (
        -- Join Standard Outputs
        -- Pass Only Elements: ['bookings', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
        -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
        -- Aggregate Measures
        SELECT
          subq_37.metric_time__day AS metric_time__day
          , listings_latest_src_28000.country AS listing__country_latest
          , SUM(subq_37.bookings) AS bookings
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
              DATE_TRUNC('day', ds) AS metric_time__day
              , listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_35
          WHERE booking__is_instant
        ) subq_37
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_37.listing = listings_latest_src_28000.listing_id
        GROUP BY
          subq_37.metric_time__day
          , listings_latest_src_28000.country
      ) subq_44
      ON
        subq_46.ds = subq_44.metric_time__day
    ) subq_47
  ) subq_48
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_64.ds AS metric_time__day
        , subq_62.listing__country_latest AS listing__country_latest
        , subq_62.bookings AS bookings
      FROM ***************************.mf_time_spine subq_64
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
            subq_54.metric_time__day AS metric_time__day
            , subq_54.booking__is_instant AS booking__is_instant
            , listings_latest_src_28000.country AS listing__country_latest
            , subq_54.bookings AS bookings
          FROM (
            -- Join to Time Spine Dataset
            -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
            SELECT
              subq_52.ds AS metric_time__day
              , subq_50.listing AS listing
              , subq_50.booking__is_instant AS booking__is_instant
              , subq_50.bookings AS bookings
            FROM ***************************.mf_time_spine subq_52
            INNER JOIN (
              -- Read Elements From Semantic Model 'bookings_source'
              -- Metric Time Dimension 'ds'
              SELECT
                DATE_TRUNC('day', ds) AS metric_time__day
                , listing_id AS listing
                , is_instant AS booking__is_instant
                , 1 AS bookings
              FROM ***************************.fct_bookings bookings_source_src_28000
            ) subq_50
            ON
              subq_52.ds - MAKE_INTERVAL(days => 14) = subq_50.metric_time__day
          ) subq_54
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            subq_54.listing = listings_latest_src_28000.listing_id
        ) subq_59
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_62
      ON
        subq_64.ds = subq_62.metric_time__day
    ) subq_65
  ) subq_66
  ON
    (
      subq_48.listing__country_latest = subq_66.listing__country_latest
    ) AND (
      subq_48.metric_time__day = subq_66.metric_time__day
    )
  GROUP BY
    COALESCE(subq_48.metric_time__day, subq_66.metric_time__day)
    , COALESCE(subq_48.listing__country_latest, subq_66.listing__country_latest)
) subq_67
