test_name: test_cumulative_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
  -- Compute Metrics via Expressions
  -- Compute Metrics via Expressions
  SELECT
    subq_28.metric_time__day AS metric_time__day
    , subq_24.__bookers AS every_2_days_bookers_2_days_ago
  FROM (
    -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
    -- Pass Only Elements: ['metric_time__day']
    SELECT
      metric_time__day
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      SELECT
        ds AS metric_time__day
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_26
    WHERE metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
  ) subq_28
  INNER JOIN (
    -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
    -- Pass Only Elements: ['__bookers', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , COUNT(DISTINCT __bookers) AS __bookers
    FROM (
      -- Join Self Over Time Range
      SELECT
        subq_20.ds AS metric_time__day
        , bookings_source_src_28000.guest_id AS __bookers
      FROM ***************************.mf_time_spine subq_20
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_20.ds
        ) AND (
          DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_20.ds - INTERVAL 2 day
        )
    ) subq_21
    WHERE metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
    GROUP BY
      metric_time__day
  ) subq_24
  ON
    subq_28.metric_time__day - INTERVAL 2 day = subq_24.metric_time__day
  WHERE subq_28.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
) subq_32
