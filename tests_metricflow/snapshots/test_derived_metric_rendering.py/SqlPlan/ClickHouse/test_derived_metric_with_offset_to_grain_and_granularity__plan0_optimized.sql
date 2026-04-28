test_name: test_derived_metric_with_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , toStartOfWeek(ds, 1) AS metric_time__week
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__week AS metric_time__week
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  SELECT
    COALESCE(subq_23.metric_time__week, subq_33.metric_time__week) AS metric_time__week
    , MAX(subq_23.bookings) AS bookings
    , MAX(subq_33.bookings_at_start_of_month) AS bookings_at_start_of_month
  FROM (
    SELECT
      metric_time__week
      , SUM(__bookings) AS bookings
    FROM sma_28009_cte
    GROUP BY
      metric_time__week
  ) subq_23
  FULL OUTER JOIN (
    SELECT
      metric_time__week
      , SUM(__bookings) AS bookings_at_start_of_month
    FROM (
      SELECT
        toStartOfWeek(time_spine_src_28006.ds, 1) AS metric_time__week
        , sma_28009_cte.__bookings AS __bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN
        sma_28009_cte
      ON
        toStartOfMonth(time_spine_src_28006.ds) = sma_28009_cte.metric_time__day
      WHERE toStartOfWeek(time_spine_src_28006.ds, 1) = time_spine_src_28006.ds
    ) subq_31
    GROUP BY
      metric_time__week
  ) subq_33
  ON
    subq_23.metric_time__week = subq_33.metric_time__week
  GROUP BY
    COALESCE(subq_23.metric_time__week, subq_33.metric_time__week)
) subq_34
