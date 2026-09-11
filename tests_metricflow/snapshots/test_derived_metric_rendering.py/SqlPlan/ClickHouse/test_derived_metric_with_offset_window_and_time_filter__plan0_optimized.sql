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
    COALESCE(subq_26.metric_time__day, subq_37.metric_time__day) AS metric_time__day
    , MAX(subq_26.bookings) AS bookings
    , MAX(subq_37.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    SELECT
      metric_time__day
      , SUM(__bookings) AS bookings
    FROM (
      SELECT
        bookings AS __bookings
        , metric_time__day
      FROM (
        SELECT
          metric_time__day
          , __bookings AS bookings
        FROM sma_28009_cte
      ) subq_22
      WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    ) subq_23
    GROUP BY
      metric_time__day
  ) subq_26
  FULL OUTER JOIN (
    SELECT
      subq_34.metric_time__day AS metric_time__day
      , subq_30.__bookings AS bookings_2_weeks_ago
    FROM (
      SELECT
        metric_time__day
      FROM (
        SELECT
          ds AS metric_time__day
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_33
      WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    ) subq_34
    INNER JOIN (
      SELECT
        metric_time__day
        , SUM(__bookings) AS __bookings
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_30
    ON
      addDays(subq_34.metric_time__day, -14) = subq_30.metric_time__day
  ) subq_37
  ON
    subq_26.metric_time__day = subq_37.metric_time__day
  GROUP BY
    COALESCE(subq_26.metric_time__day, subq_37.metric_time__day)
) subq_38
SETTINGS join_use_nulls = 1
