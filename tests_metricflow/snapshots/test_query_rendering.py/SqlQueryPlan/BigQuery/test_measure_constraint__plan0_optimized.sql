-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_41.metric_time__day, subq_46.metric_time__day) AS metric_time__day
    , MAX(subq_41.average_booking_value) AS average_booking_value
    , MAX(subq_41.bookings) AS bookings
    , MAX(subq_46.booking_value) AS booking_value
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
        subq_32.metric_time__day AS metric_time__day
        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , subq_32.bookings AS bookings
        , subq_32.average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['average_booking_value', 'bookings', 'metric_time__day', 'listing']
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
          , booking_value AS average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_32
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_32.listing = listings_latest_src_28000.listing_id
    ) subq_37
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_41
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , SUM(booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      metric_time__day
  ) subq_46
  ON
    subq_41.metric_time__day = subq_46.metric_time__day
  GROUP BY
    metric_time__day
) subq_47
