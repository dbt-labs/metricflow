-- Compute Metrics via Expressions
SELECT
  instant_lux_booking_value_rate AS instant_lux_booking_value_rate
FROM (
  -- Compute Metrics via Expressions
  SELECT
    average_booking_value * bookings / NULLIF(booking_value, 0) AS instant_lux_booking_value_rate
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      MAX(subq_60.average_booking_value) AS average_booking_value
      , MAX(subq_73.bookings) AS bookings
      , MAX(subq_81.booking_value) AS booking_value
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['average_booking_value',]
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        AVG(average_booking_value) AS average_booking_value
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements: ['average_booking_value', 'listing__is_lux_latest', 'booking__is_instant']
        SELECT
          subq_51.booking__is_instant AS booking__is_instant
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , subq_51.average_booking_value AS average_booking_value
        FROM (
          -- Constrain Output with WHERE
          -- Pass Only Elements: ['average_booking_value', 'booking__is_instant', 'listing']
          SELECT
            listing
            , booking__is_instant
            , average_booking_value
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              listing_id AS listing
              , is_instant AS booking__is_instant
              , booking_value AS average_booking_value
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_49
          WHERE booking__is_instant
        ) subq_51
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_51.listing = listings_latest_src_28000.listing_id
      ) subq_56
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) subq_60
    CROSS JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['bookings',]
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        SUM(bookings) AS bookings
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements: ['bookings', 'listing__is_lux_latest', 'booking__is_instant']
        SELECT
          subq_64.booking__is_instant AS booking__is_instant
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , subq_64.bookings AS bookings
        FROM (
          -- Constrain Output with WHERE
          -- Pass Only Elements: ['bookings', 'booking__is_instant', 'listing']
          SELECT
            listing
            , booking__is_instant
            , bookings
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_62
          WHERE booking__is_instant
        ) subq_64
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_64.listing = listings_latest_src_28000.listing_id
      ) subq_69
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) subq_73
    CROSS JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['booking_value',]
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        SUM(booking_value) AS booking_value
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['booking_value', 'booking__is_instant']
        SELECT
          booking__is_instant
          , booking_value
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            is_instant AS booking__is_instant
            , booking_value
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_75
        WHERE booking__is_instant
      ) subq_77
      WHERE booking__is_instant
    ) subq_81
  ) subq_82
) subq_83
