test_name: test_simple_metric_constraint
test_filename: test_query_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_37.metric_time__day, subq_43.metric_time__day) AS metric_time__day
    , MAX(subq_37.average_booking_value) AS average_booking_value
    , MAX(subq_37.bookings) AS bookings
    , MAX(subq_43.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__average_booking_value', '__bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , AVG(average_booking_value) AS average_booking_value
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['__average_booking_value', '__bookings', 'listing__is_lux_latest', 'metric_time__day']
      SELECT
        subq_28.metric_time__day AS metric_time__day
        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , subq_28.__bookings AS bookings
        , subq_28.__average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __bookings
          , booking_value AS __average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_28
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_28.listing = listings_latest_src_28000.listing_id
    ) subq_33
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_37
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__booking_value', 'metric_time__day']
    -- Pass Only Elements: ['__booking_value', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , SUM(booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      DATE_TRUNC('day', ds)
  ) subq_43
  ON
    subq_37.metric_time__day = subq_43.metric_time__day
  GROUP BY
    COALESCE(subq_37.metric_time__day, subq_43.metric_time__day)
) subq_44
