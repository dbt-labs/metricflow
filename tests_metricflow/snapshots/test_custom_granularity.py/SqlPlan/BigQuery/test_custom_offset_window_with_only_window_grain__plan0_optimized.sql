test_name: test_custom_offset_window_with_only_window_grain
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__alien_day
  , metric_time__alien_day
  , bookings AS bookings_offset_one_alien_day
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__alien_day', 'booking__ds__alien_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_14.booking__ds__alien_day AS booking__ds__alien_day
    , subq_14.metric_time__alien_day AS metric_time__alien_day
    , SUM(subq_12.bookings) AS bookings
  FROM (
    -- Join Offset Custom Granularity to Base Granularity
    WITH cte_6 AS (
      -- Read From Time Spine 'mf_time_spine'
      SELECT
        ds AS ds__day
        , alien_day AS ds__alien_day
      FROM ***************************.mf_time_spine time_spine_src_28006
    )

    SELECT
      cte_6.ds__day AS ds__day
      , subq_13.ds__alien_day__lead AS booking__ds__alien_day
      , subq_13.ds__alien_day__lead AS metric_time__alien_day
    FROM cte_6 cte_6
    INNER JOIN (
      -- Offset Custom Granularity
      SELECT
        ds__alien_day
        , LEAD(ds__alien_day, 1) OVER (ORDER BY ds__alien_day) AS ds__alien_day__lead
      FROM cte_6 cte_6
      GROUP BY
        ds__alien_day
    ) subq_13
    ON
      cte_6.ds__alien_day = subq_13.ds__alien_day
  ) subq_14
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  ON
    subq_14.ds__day = subq_12.metric_time__day
  GROUP BY
    booking__ds__alien_day
    , metric_time__alien_day
) subq_19
