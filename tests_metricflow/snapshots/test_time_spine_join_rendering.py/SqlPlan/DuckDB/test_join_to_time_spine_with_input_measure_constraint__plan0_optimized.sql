test_name: test_join_to_time_spine_with_input_measure_constraint
test_filename: test_time_spine_join_rendering.py
docstring:
  Check filter hierarchy.

      Ensure that the measure filter 'booking__is_instant' doesn't get applied again post-aggregation.
sql_engine: DuckDB
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_19.metric_time__day AS metric_time__day
  , subq_15.booking__is_instant AS booking__is_instant
  , subq_15.instant_bookings_with_measure_filter AS instant_bookings_with_measure_filter
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day']
  SELECT
    metric_time__day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_17
  WHERE metric_time__day > '2020-01-01'
) subq_19
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['instant_bookings_with_measure_filter', 'booking__is_instant', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    metric_time__day
    , booking__is_instant
    , SUM(instant_bookings_with_measure_filter) AS instant_bookings_with_measure_filter
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , is_instant AS booking__is_instant
      , 1 AS instant_bookings_with_measure_filter
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  WHERE ((booking__is_instant) AND (listing IS NOT NULL)) AND (metric_time__day > '2020-01-01')
  GROUP BY
    metric_time__day
    , booking__is_instant
) subq_15
ON
  subq_19.metric_time__day = subq_15.metric_time__day
