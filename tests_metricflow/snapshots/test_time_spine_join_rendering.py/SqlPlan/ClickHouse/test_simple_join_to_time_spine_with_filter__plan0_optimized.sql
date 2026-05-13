test_name: test_simple_join_to_time_spine_with_filter
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric fills nulls and filter is not in group by. Should apply constraint once.
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_17.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    SELECT
      metric_time__day
      , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
    FROM (
      SELECT
        metric_time__day
        , bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
      FROM (
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , is_instant AS booking__is_instant
          , 1 AS bookings_fill_nulls_with_0
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_14
      WHERE booking__is_instant
    ) subq_16
    GROUP BY
      metric_time__day
  ) subq_17
  ON
    time_spine_src_28006.ds = subq_17.metric_time__day
) subq_22
