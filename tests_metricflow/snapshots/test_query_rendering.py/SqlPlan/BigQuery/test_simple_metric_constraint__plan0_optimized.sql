test_name: test_simple_metric_constraint
test_filename: test_query_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_33.metric_time__day, subq_38.metric_time__day) AS metric_time__day
    , MAX(subq_33.average_booking_value) AS average_booking_value
    , MAX(subq_33.bookings) AS bookings
    , MAX(subq_38.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['average_booking_value', 'bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , AVG(average_booking_value) AS average_booking_value
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , subq_25.metric_time__day AS metric_time__day
        , subq_25.bookings AS bookings
        , subq_25.average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
          , booking_value AS average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_25
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_25.listing = listings_latest_src_28000.listing_id
    ) subq_29
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_33
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , SUM(booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      metric_time__day
  ) subq_38
  ON
    subq_33.metric_time__day = subq_38.metric_time__day
  GROUP BY
    metric_time__day
) subq_39
