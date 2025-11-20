test_name: test_join_to_time_spine_with_filters
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  SELECT
    subq_25.metric_time__day AS metric_time__day
    , subq_20.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
  FROM (
    -- Constrain Output with WHERE
    -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
    -- Pass Only Elements: ['metric_time__day']
    SELECT
      metric_time__day
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      SELECT
        ds AS metric_time__day
        , DATE_TRUNC('week', ds) AS metric_time__week
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_22
    WHERE (
      metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
    ) AND (
      metric_time__week > '2020-01-01'
    )
  ) subq_25
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , DATE_TRUNC('week', ds) AS metric_time__week
        , 1 AS bookings_fill_nulls_with_0
      FROM ***************************.fct_bookings bookings_source_src_28000
      WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-03' AND '2020-01-05'
    ) subq_17
    WHERE metric_time__week > '2020-01-01'
    GROUP BY
      metric_time__day
  ) subq_20
  ON
    subq_25.metric_time__day = subq_20.metric_time__day
  WHERE subq_25.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
) subq_27
