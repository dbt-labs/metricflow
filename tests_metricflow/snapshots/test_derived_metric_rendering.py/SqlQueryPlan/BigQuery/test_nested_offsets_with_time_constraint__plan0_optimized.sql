test_name: test_nested_offsets_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2020-01-12T00:00:00, 2020-01-13T00:00:00]
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_24.bookings_offset_once AS bookings_offset_once
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , 2 * bookings AS bookings_offset_once
    FROM (
      -- Join to Time Spine Dataset
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        time_spine_src_28006.ds AS metric_time__day
        , SUM(subq_16.bookings) AS bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_16
      ON
        DATE_SUB(CAST(time_spine_src_28006.ds AS DATETIME), INTERVAL 5 day) = subq_16.metric_time__day
      GROUP BY
        metric_time__day
    ) subq_23
  ) subq_24
  ON
    DATE_SUB(CAST(time_spine_src_28006.ds AS DATETIME), INTERVAL 2 day) = subq_24.metric_time__day
  WHERE time_spine_src_28006.ds BETWEEN '2020-01-12' AND '2020-01-13'
) subq_29
