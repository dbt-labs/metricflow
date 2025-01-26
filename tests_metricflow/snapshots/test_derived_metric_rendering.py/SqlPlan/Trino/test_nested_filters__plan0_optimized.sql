test_name: test_nested_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests derived metric rendering for a nested derived metric with filters on the outer metric spec.
sql_engine: Trino
---
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
      MAX(subq_34.average_booking_value) AS average_booking_value
      , MAX(subq_34.bookings) AS bookings
      , MAX(subq_40.booking_value) AS booking_value
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
        SELECT
          listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , subq_26.booking__is_instant AS booking__is_instant
          , subq_26.bookings AS bookings
          , subq_26.average_booking_value AS average_booking_value
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            listing_id AS listing
            , is_instant AS booking__is_instant
            , 1 AS bookings
            , booking_value AS average_booking_value
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_26
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_26.listing = listings_latest_src_28000.listing_id
      ) subq_30
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) subq_34
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
        SELECT
          is_instant AS booking__is_instant
          , booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_36
      WHERE booking__is_instant
    ) subq_40
  ) subq_41
) subq_42
