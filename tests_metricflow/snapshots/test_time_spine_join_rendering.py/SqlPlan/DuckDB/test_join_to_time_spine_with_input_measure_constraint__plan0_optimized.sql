test_name: test_join_to_time_spine_with_input_measure_constraint
test_filename: test_time_spine_join_rendering.py
docstring:
  Check filter hierarchy.

      Ensure that the measure filter 'booking__is_instant' doesn't get applied again post-aggregation.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , booking__is_instant
  , SUM(bookings) AS instant_bookings_with_measure_filter
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , is_instant AS booking__is_instant
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_7
WHERE ((((booking__is_instant) AND (listing IS NOT NULL)) AND (booking__is_instant)) AND (listing IS NOT NULL)) AND (metric_time__day > '2020-01-01')
GROUP BY
  metric_time__day
  , booking__is_instant
