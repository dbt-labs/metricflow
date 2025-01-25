test_name: test_nested_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests derived metric rendering for a nested derived metric with filters on the outer metric spec.
sql_engine: Databricks
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
      MAX(nr_subq_32.average_booking_value) AS average_booking_value
      , MAX(nr_subq_32.bookings) AS bookings
      , MAX(nr_subq_37.booking_value) AS booking_value
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
          , nr_subq_24.booking__is_instant AS booking__is_instant
          , nr_subq_24.bookings AS bookings
          , nr_subq_24.average_booking_value AS average_booking_value
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            listing_id AS listing
            , is_instant AS booking__is_instant
            , 1 AS bookings
            , booking_value AS average_booking_value
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) nr_subq_24
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          nr_subq_24.listing = listings_latest_src_28000.listing_id
      ) nr_subq_28
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) nr_subq_32
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
      ) nr_subq_33
      WHERE booking__is_instant
    ) nr_subq_37
  ) nr_subq_38
) nr_subq_39
