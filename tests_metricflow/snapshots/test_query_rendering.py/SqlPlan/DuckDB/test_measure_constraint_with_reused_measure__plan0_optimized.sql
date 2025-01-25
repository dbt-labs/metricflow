test_name: test_measure_constraint_with_reused_measure
test_filename: test_query_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(booking_value_with_is_instant_constraint AS DOUBLE) / CAST(NULLIF(booking_value, 0) AS DOUBLE) AS instant_booking_value_ratio
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_14.metric_time__day, nr_subq_18.metric_time__day) AS metric_time__day
    , MAX(nr_subq_14.booking_value_with_is_instant_constraint) AS booking_value_with_is_instant_constraint
    , MAX(nr_subq_18.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(booking_value) AS booking_value_with_is_instant_constraint
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , is_instant AS booking__is_instant
        , booking_value
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_10
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) nr_subq_14
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
  ) nr_subq_18
  ON
    nr_subq_14.metric_time__day = nr_subq_18.metric_time__day
  GROUP BY
    COALESCE(nr_subq_14.metric_time__day, nr_subq_18.metric_time__day)
) nr_subq_19
