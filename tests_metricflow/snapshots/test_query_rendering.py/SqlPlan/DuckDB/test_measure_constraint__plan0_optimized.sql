test_name: test_measure_constraint
test_filename: test_query_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['average_booking_value', 'bookings', 'booking_value', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , AVG(average_booking_value) AS average_booking_value
    , SUM(bookings) AS bookings
    , SUM(booking_value) AS booking_value
  FROM (
    -- Join Standard Outputs
    SELECT
      listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , subq_28.metric_time__day AS metric_time__day
      , subq_28.bookings AS bookings
      , subq_28.booking_value AS booking_value
      , subq_28.average_booking_value AS average_booking_value
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
        , booking_value
        , booking_value AS average_booking_value
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_28
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_28.listing = listings_latest_src_28000.listing_id
  ) subq_32
  WHERE listing__is_lux_latest
  GROUP BY
    metric_time__day
) subq_36
