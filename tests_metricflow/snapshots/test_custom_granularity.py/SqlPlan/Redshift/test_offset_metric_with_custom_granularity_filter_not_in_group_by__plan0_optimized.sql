test_name: test_offset_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  SELECT
    subq_26.metric_time__day AS metric_time__day
    , subq_21.__bookings AS bookings_5_days_ago
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['metric_time__day']
    SELECT
      metric_time__day
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      -- Pass Only Elements: ['metric_time__day', 'metric_time__alien_day']
      SELECT
        ds AS metric_time__day
        , alien_day AS metric_time__alien_day
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_24
    WHERE metric_time__alien_day = '2020-01-01'
  ) subq_26
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings) AS __bookings
    FROM (
      -- Metric Time Dimension 'ds'
      -- Join to Custom Granularity Dataset
      -- Pass Only Elements: ['__bookings', 'metric_time__day', 'metric_time__alien_day']
      SELECT
        subq_16.alien_day AS metric_time__alien_day
        , subq_15.ds__day AS metric_time__day
        , subq_15.__bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        SELECT
          1 AS __bookings
          , DATE_TRUNC('day', ds) AS ds__day
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_15
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_16
      ON
        subq_15.ds__day = subq_16.ds
    ) subq_18
    WHERE metric_time__alien_day = '2020-01-01'
    GROUP BY
      metric_time__day
  ) subq_21
  ON
    DATEADD(day, -5, subq_26.metric_time__day) = subq_21.metric_time__day
) subq_28
