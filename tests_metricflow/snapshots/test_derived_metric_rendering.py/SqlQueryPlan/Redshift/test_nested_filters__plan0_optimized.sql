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
      MAX(subq_45.average_booking_value) AS average_booking_value
      , MAX(subq_45.bookings) AS bookings
      , MAX(subq_52.booking_value) AS booking_value
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['average_booking_value', 'bookings']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        AVG(average_booking_value) AS average_booking_value
        , SUM(bookings) AS bookings
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements: ['average_booking_value', 'bookings', 'listing__is_lux_latest', 'booking__is_instant']
        SELECT
          listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , subq_36.bookings AS bookings
          , subq_36.average_booking_value AS average_booking_value
        FROM (
          -- Constrain Output with WHERE
          -- Pass Only Elements: ['average_booking_value', 'bookings', 'booking__is_instant', 'listing']
          SELECT
            listing
            , bookings
            , average_booking_value
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
              , booking_value AS average_booking_value
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_34
          WHERE booking__is_instant
        ) subq_36
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_36.listing = listings_latest_src_28000.listing_id
      ) subq_41
      WHERE listing__is_lux_latest
    ) subq_45
    CROSS JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['booking_value', 'booking__is_instant']
      -- Pass Only Elements: ['booking_value',]
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        SUM(booking_value) AS booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          is_instant AS booking__is_instant
          , booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_47
      WHERE booking__is_instant
    ) subq_52
  ) subq_53
) subq_54
