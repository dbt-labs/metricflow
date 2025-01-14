test_name: test_join_to_time_spine_with_filters
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  SELECT
    subq_23.metric_time__day AS metric_time__day
    , subq_18.bookings AS bookings
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
        , date_trunc('week', ds) AS metric_time__week
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_20
    WHERE (
      metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
    ) AND (
      metric_time__week > '2020-01-01'
    )
  ) subq_23
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
        date_trunc('day', ds) AS metric_time__day
        , date_trunc('week', ds) AS metric_time__week
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
      WHERE date_trunc('day', ds) BETWEEN '2020-01-03' AND '2020-01-05'
    ) subq_15
    WHERE metric_time__week > '2020-01-01'
    GROUP BY
      metric_time__day
  ) subq_18
  ON
    subq_23.metric_time__day = subq_18.metric_time__day
  WHERE subq_23.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
) subq_25
