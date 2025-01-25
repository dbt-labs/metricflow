test_name: test_simple_join_to_time_spine_pushdown_filter_application
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
sql_engine: Redshift
---
-- Constrain Output with WHERE
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , booking__is_instant
  , bookings AS bookings_join_to_time_spine
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , nr_subq_12.booking__is_instant AS booking__is_instant
    , nr_subq_12.bookings AS bookings
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , booking__is_instant
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_9
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , booking__is_instant
  ) nr_subq_12
  ON
    time_spine_src_28006.ds = nr_subq_12.metric_time__day
) nr_subq_16
WHERE booking__is_instant
