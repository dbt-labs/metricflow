-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_28.metric_time__day, subq_32.metric_time__day) AS metric_time__day
    , MAX(subq_28.average_booking_value) AS average_booking_value
    , MAX(subq_28.bookings) AS bookings
    , MAX(subq_32.booking_value) AS booking_value
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
      -- Pass Only Elements: ['average_booking_value', 'bookings', 'listing__is_lux_latest', 'metric_time__day']
      SELECT
        subq_22.metric_time__day AS metric_time__day
        , subq_22.bookings AS bookings
        , subq_22.average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['average_booking_value', 'bookings', 'metric_time__day', 'listing']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
          , booking_value AS average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_22
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_22.listing = listings_latest_src_28000.listing_id
    ) subq_25
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_28
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
  ) subq_32
  ON
    subq_28.metric_time__day = subq_32.metric_time__day
  GROUP BY
    COALESCE(subq_28.metric_time__day, subq_32.metric_time__day)
) subq_33
