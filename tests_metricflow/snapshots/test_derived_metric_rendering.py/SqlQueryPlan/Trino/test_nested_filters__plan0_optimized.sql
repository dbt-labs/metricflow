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
      MAX(subq_48.average_booking_value) AS average_booking_value
      , MAX(subq_61.bookings) AS bookings
      , MAX(subq_69.booking_value) AS booking_value
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
          subq_39.booking__is_instant AS booking__is_instant
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , subq_39.average_booking_value AS average_booking_value
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
          ) subq_37
          WHERE booking__is_instant
        ) subq_39
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_39.listing = listings_latest_src_28000.listing_id
      ) subq_44
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) subq_48
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
          subq_52.booking__is_instant AS booking__is_instant
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , subq_52.bookings AS bookings
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
          ) subq_50
          WHERE booking__is_instant
        ) subq_52
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_52.listing = listings_latest_src_28000.listing_id
      ) subq_57
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) subq_61
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
        ) subq_63
        WHERE booking__is_instant
      ) subq_65
      WHERE booking__is_instant
    ) subq_69
  ) subq_70
) subq_71
