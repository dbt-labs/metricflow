test_name: test_join_to_time_spine_with_filters
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  SELECT
    nr_subq_21.metric_time__day AS metric_time__day
    , nr_subq_16.bookings AS bookings
  FROM (
    -- Constrain Output with WHERE
    -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
    -- Pass Only Elements: ['metric_time__day',]
    SELECT
      metric_time__day
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      SELECT
        ds AS metric_time__day
        , DATE_TRUNC('week', ds) AS metric_time__week
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) nr_subq_18
    WHERE (
      metric_time__day BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
    ) AND (
      metric_time__week > '2020-01-01'
    )
  ) nr_subq_21
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , DATE_TRUNC('week', ds) AS metric_time__week
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
      WHERE DATE_TRUNC('day', ds) BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
    ) nr_subq_13
    WHERE metric_time__week > '2020-01-01'
    GROUP BY
      metric_time__day
  ) nr_subq_16
  ON
    nr_subq_21.metric_time__day = nr_subq_16.metric_time__day
  WHERE nr_subq_21.metric_time__day BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
) nr_subq_23
