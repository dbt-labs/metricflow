test_name: test_join_to_time_spine_with_queried_time_constraint
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric that fills nulls is queried with metric time and a time constraint. Should apply constraint twice.
sql_engine: Trino
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  SELECT
    subq_21.metric_time__day AS metric_time__day
    , subq_17.bookings AS bookings
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
    -- Pass Only Elements: ['metric_time__day']
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine time_spine_src_28006
    WHERE ds BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
  ) subq_21
  LEFT OUTER JOIN (
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
      WHERE DATE_TRUNC('day', ds) BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
    ) subq_16
    GROUP BY
      metric_time__day
  ) subq_17
  ON
    subq_21.metric_time__day = subq_17.metric_time__day
  WHERE subq_21.metric_time__day BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
) subq_23
