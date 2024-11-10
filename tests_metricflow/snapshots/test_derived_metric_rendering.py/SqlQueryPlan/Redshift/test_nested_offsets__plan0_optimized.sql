test_name: test_nested_offsets
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_22.ds AS metric_time__day
    , subq_20.bookings_offset_once AS bookings_offset_once
  FROM ***************************.mf_time_spine subq_22
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
        subq_15.ds AS metric_time__day
        , SUM(subq_13.bookings) AS bookings
      FROM ***************************.mf_time_spine subq_15
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_13
      ON
        DATEADD(day, -5, subq_15.ds) = subq_13.metric_time__day
      GROUP BY
        subq_15.ds
    ) subq_19
  ) subq_20
  ON
    DATEADD(day, -2, subq_22.ds) = subq_20.metric_time__day
) subq_23
