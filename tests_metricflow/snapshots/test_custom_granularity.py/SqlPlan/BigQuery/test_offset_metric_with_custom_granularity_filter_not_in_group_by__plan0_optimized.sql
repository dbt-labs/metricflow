test_name: test_offset_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: BigQuery
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
    subq_22.metric_time__day AS metric_time__day
    , subq_18.bookings AS bookings_5_days_ago
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['metric_time__day']
    SELECT
      metric_time__day
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      SELECT
        ds AS metric_time__day
        , alien_day AS metric_time__alien_day
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_20
    WHERE metric_time__alien_day = '2020-01-01'
  ) subq_22
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Metric Time Dimension 'ds'
      -- Join to Custom Granularity Dataset
      SELECT
        subq_13.ds__day AS metric_time__day
        , subq_13.bookings AS bookings
        , subq_14.alien_day AS metric_time__alien_day
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        SELECT
          1 AS bookings
          , DATETIME_TRUNC(ds, day) AS ds__day
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_13
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_14
      ON
        subq_13.ds__day = subq_14.ds
    ) subq_15
    WHERE metric_time__alien_day = '2020-01-01'
    GROUP BY
      metric_time__day
  ) subq_18
  ON
    DATE_SUB(CAST(subq_22.metric_time__day AS DATETIME), INTERVAL 5 day) = subq_18.metric_time__day
) subq_24
