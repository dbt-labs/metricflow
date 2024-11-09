test_name: test_measure_constraint
test_filename: test_query_rendering.py
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_35.metric_time__day, subq_40.metric_time__day) AS metric_time__day
    , MAX(subq_35.average_booking_value) AS average_booking_value
    , MAX(subq_35.bookings) AS bookings
    , MAX(subq_40.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['average_booking_value', 'bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , AVG(average_booking_value) AS average_booking_value
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , subq_27.metric_time__day AS metric_time__day
        , subq_27.bookings AS bookings
        , subq_27.average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
          , booking_value AS average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_27
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_27.listing = listings_latest_src_28000.listing_id
    ) subq_31
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_35
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , SUM(booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      DATE_TRUNC('day', ds)
  ) subq_40
  ON
    subq_35.metric_time__day = subq_40.metric_time__day
  GROUP BY
    COALESCE(subq_35.metric_time__day, subq_40.metric_time__day)
) subq_41
