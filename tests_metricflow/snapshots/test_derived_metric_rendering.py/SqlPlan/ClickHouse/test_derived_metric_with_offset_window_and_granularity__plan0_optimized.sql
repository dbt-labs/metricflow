test_name: test_derived_metric_with_offset_window_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , toStartOfQuarter(ds) AS metric_time__quarter
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__quarter AS metric_time__quarter
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  SELECT
    COALESCE(subq_23.metric_time__quarter, subq_33.metric_time__quarter) AS metric_time__quarter
    , MAX(subq_23.bookings) AS bookings
    , MAX(subq_33.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    SELECT
      metric_time__quarter
      , SUM(__bookings) AS bookings
    FROM sma_28009_cte
    GROUP BY
      metric_time__quarter
  ) subq_23
  FULL OUTER JOIN (
    SELECT
      toStartOfQuarter(time_spine_src_28006.ds) AS metric_time__quarter
      , SUM(sma_28009_cte.__bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte
    ON
      addDays(time_spine_src_28006.ds, -14) = sma_28009_cte.metric_time__day
    GROUP BY
      toStartOfQuarter(time_spine_src_28006.ds)
  ) subq_33
  ON
    subq_23.metric_time__quarter = subq_33.metric_time__quarter
  GROUP BY
    COALESCE(subq_23.metric_time__quarter, subq_33.metric_time__quarter)
) subq_34
