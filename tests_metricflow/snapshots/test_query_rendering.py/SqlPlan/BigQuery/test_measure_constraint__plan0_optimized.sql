test_name: test_measure_constraint
test_filename: test_query_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_30.metric_time__day, nr_subq_34.metric_time__day) AS metric_time__day
    , MAX(nr_subq_30.average_booking_value) AS average_booking_value
    , MAX(nr_subq_30.bookings) AS bookings
    , MAX(nr_subq_34.booking_value) AS booking_value
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
        , nr_subq_22.metric_time__day AS metric_time__day
        , nr_subq_22.bookings AS bookings
        , nr_subq_22.average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
          , booking_value AS average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) nr_subq_22
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        nr_subq_22.listing = listings_latest_src_28000.listing_id
    ) nr_subq_26
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) nr_subq_30
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
  ) nr_subq_34
  ON
    nr_subq_30.metric_time__day = nr_subq_34.metric_time__day
  GROUP BY
    metric_time__day
) nr_subq_35
