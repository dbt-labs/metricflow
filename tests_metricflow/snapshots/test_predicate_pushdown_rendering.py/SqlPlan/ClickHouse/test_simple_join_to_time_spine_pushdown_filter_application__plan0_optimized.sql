test_name: test_simple_join_to_time_spine_pushdown_filter_application
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , booking__is_instant
  , bookings_join_to_time_spine
FROM (
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_18.booking__is_instant AS booking__is_instant
    , subq_18.__bookings_join_to_time_spine AS bookings_join_to_time_spine
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    SELECT
      metric_time__day
      , booking__is_instant
      , SUM(__bookings_join_to_time_spine) AS __bookings_join_to_time_spine
    FROM (
      SELECT
        metric_time__day
        , booking__is_instant
        , bookings_join_to_time_spine AS __bookings_join_to_time_spine
      FROM (
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , is_instant AS booking__is_instant
          , 1 AS bookings_join_to_time_spine
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_15
      WHERE booking__is_instant
    ) subq_17
    GROUP BY
      metric_time__day
      , booking__is_instant
  ) subq_18
  ON
    time_spine_src_28006.ds = subq_18.metric_time__day
) subq_23
WHERE booking__is_instant
