test_name: test_custom_offset_window_with_only_window_grain
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
SELECT
  metric_time__alien_day
  , booking__ds__alien_day
  , bookings AS bookings_offset_one_alien_day
FROM (
  SELECT
    subq_17.metric_time__alien_day AS metric_time__alien_day
    , subq_17.booking__ds__alien_day AS booking__ds__alien_day
    , SUM(subq_15.__bookings) AS bookings
  FROM (
    WITH cte_6 AS (
      SELECT
        ds AS ds__day
        , alien_day AS ds__alien_day
      FROM ***************************.mf_time_spine time_spine_src_28006
    )

    SELECT
      cte_6.ds__day AS ds__day
      , subq_16.ds__alien_day__lead AS metric_time__alien_day
      , subq_16.ds__alien_day__lead AS booking__ds__alien_day
    FROM cte_6
    INNER JOIN (
      SELECT
        ds__alien_day
        , LEAD(ds__alien_day, 1) OVER (ORDER BY ds__alien_day) AS ds__alien_day__lead
      FROM cte_6
      GROUP BY
        ds__alien_day
    ) subq_16
    ON
      cte_6.ds__alien_day = subq_16.ds__alien_day
  ) subq_17
  INNER JOIN (
    SELECT
      toStartOfDay(ds) AS metric_time__day
      , 1 AS __bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_15
  ON
    subq_17.ds__day = subq_15.metric_time__day
  GROUP BY
    subq_17.metric_time__alien_day
    , subq_17.booking__ds__alien_day
) subq_24
