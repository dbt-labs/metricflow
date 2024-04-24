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
      MAX(subq_44.average_booking_value) AS average_booking_value
      , MAX(subq_56.bookings) AS bookings
      , MAX(subq_63.booking_value) AS booking_value
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
          bookings_source_src_28000.is_instant AS booking__is_instant
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , bookings_source_src_28000.booking_value AS average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          bookings_source_src_28000.listing_id = listings_latest_src_28000.listing_id
      ) subq_40
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) subq_44
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
          subq_47.booking__is_instant AS booking__is_instant
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , subq_47.bookings AS bookings
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['bookings', 'booking__is_instant', 'listing']
          SELECT
            listing_id AS listing
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_47
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_47.listing = listings_latest_src_28000.listing_id
      ) subq_52
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) subq_56
    CROSS JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['booking_value',]
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        SUM(booking_value) AS booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['booking_value', 'booking__is_instant']
        SELECT
          is_instant AS booking__is_instant
          , booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_59
      WHERE booking__is_instant
    ) subq_63
  ) subq_64
) subq_65
