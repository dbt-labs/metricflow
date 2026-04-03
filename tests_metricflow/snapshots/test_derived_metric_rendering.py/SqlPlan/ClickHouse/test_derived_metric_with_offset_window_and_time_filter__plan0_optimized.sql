test_name: test_derived_metric_with_offset_window_and_time_filter
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  SELECT
    COALESCE(subq_27.metric_time__day, subq_39.metric_time__day) AS metric_time__day
    , MAX(subq_27.bookings) AS bookings
    , MAX(subq_39.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    SELECT
      metric_time__day
      , SUM(__bookings) AS bookings
    FROM (
      SELECT
        metric_time__day
        , bookings AS __bookings
      FROM (
        SELECT
          metric_time__day
          , __bookings AS bookings
        FROM sma_28009_cte
      ) subq_23
      WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    ) subq_25
    GROUP BY
      metric_time__day
  ) subq_27
  FULL OUTER JOIN (
    SELECT
      subq_37.metric_time__day AS metric_time__day
      , subq_32.__bookings AS bookings_2_weeks_ago
    FROM (
      SELECT
        metric_time__day
      FROM (
        SELECT
          ds AS metric_time__day
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_35
      WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    ) subq_37
    INNER JOIN (
      SELECT
        metric_time__day
        , SUM(__bookings) AS __bookings
      FROM (
        SELECT
          metric_time__day
          , bookings AS __bookings
        FROM (
          SELECT
            metric_time__day
            , __bookings AS bookings
          FROM sma_28009_cte
        ) subq_29
        WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
      ) subq_31
      GROUP BY
        metric_time__day
    ) subq_32
    ON
      addDays(subq_37.metric_time__day, -14) = subq_32.metric_time__day
  ) subq_39
  ON
    subq_27.metric_time__day = subq_39.metric_time__day
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_39.metric_time__day)
) subq_40
